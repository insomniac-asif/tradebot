/* ═══════════════════════════════════════════════════════ SpyBot Dashboard */
'use strict';

const POLL_INTERVAL = 30000; // 30s
let symbolCharts = {};   // { SPY: ApexChartsInstance, ... }
let _focusedSym = null;
const FOCUSED_CHART_H = 300;
let perfChart = null;
let simsCache = [];
let currentSimId = null;
let _symbolRegistryCache = null;

// ─────────────────────────────────────────────── INIT
document.addEventListener('DOMContentLoaded', async () => {
  startClock();
  await initSymbolCharts();
  refreshAll();
  setInterval(refreshAll, POLL_INTERVAL);
});

async function refreshAll() {
  await Promise.all([
    fetchStatus(),
    fetchSims(),
    fetchChartAndPredictions(),
    fetchRecentTrades(),
  ]);
  updateRefreshTime();
}

function updateRefreshTime() {
  const el = document.getElementById('last-refresh');
  const now = new Date();
  el.textContent = now.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

// ─────────────────────────────────────────────── STATUS
async function fetchStatus() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();

    const dot = document.getElementById('bot-dot');
    const txt = document.getElementById('bot-status-text');
    const mkt = document.getElementById('market-status');

    dot.className = 'bot-dot ' + (d.alive ? 'alive' : 'dead');
    if (d.alive) {
      txt.textContent = `Bot alive · ${d.age_seconds}s ago`;
    } else if (d.last_heartbeat) {
      txt.textContent = `Bot offline · ${d.age_seconds}s ago`;
    } else {
      txt.textContent = 'Bot offline';
    }

    if (d.market_open) {
      mkt.textContent = `🟢 Market Open · ${d.market_time}`;
      mkt.className = 'market-open';
    } else {
      mkt.textContent = `⭕ Market Closed · ${d.market_time}`;
      mkt.className = 'market-close';
    }
  } catch (e) {
    document.getElementById('bot-status-text').textContent = 'Fetch error';
  }
}

// ─────────────────────────────────────────────── SIMS GRID
async function fetchSims() {
  try {
    const r = await fetch('/api/sims');
    simsCache = await r.json();
    renderDesks(simsCache);
    renderTeacherDesk(simsCache);
  } catch (e) {
    document.getElementById('desks-grid').innerHTML =
      '<div class="loading-msg text-red">Failed to load sims</div>';
  }
}

// ─────────────────────────────────────────────── ET CLOCK
function startClock() {
  function tick() {
    const el = document.getElementById('et-clock');
    if (!el) return;
    const now = new Date();
    const et = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
    const h = et.getHours(), m = et.getMinutes(), s = et.getSeconds();
    const pad = n => String(n).padStart(2, '0');
    const timeStr = `${pad(h)}:${pad(m)}:${pad(s)}`;
    const totalMin = h * 60 + m;
    let session;
    if (totalMin < 9 * 60 + 30)       session = 'Pre-Market';
    else if (totalMin < 16 * 60)       session = 'Market Open';
    else if (totalMin < 20 * 60)       session = 'After-Hours';
    else                               session = 'Closed';
    el.textContent = `${timeStr} ET · ${session}`;
  }
  tick();
  setInterval(tick, 1000);
}

// ─────────────────────────────────────────────── TEACHER DESK
function renderTeacherDesk(sims) {
  const panel = document.getElementById('teacher-panel');
  if (!panel || !sims.length) return;

  const totalPnl    = sims.reduce((s, x) => s + (x.pnl_dollars || 0), 0);
  const totalBal    = sims.reduce((s, x) => s + (x.balance || 0), 0);
  const totalTrades = sims.reduce((s, x) => s + (x.total_trades || 0), 0);
  const activeCount = sims.filter(x => x.open_count > 0).length;
  const wrs         = sims.map(x => x.win_rate).filter(v => v != null && v !== '' && !isNaN(v));
  const avgWr       = wrs.length ? (wrs.reduce((a, b) => a + parseFloat(b), 0) / wrs.length) : null;

  const pnlSign  = totalPnl >= 0 ? '+' : '';
  const pnlClass = totalPnl > 0 ? 'profit' : totalPnl < 0 ? 'loss' : '';

  panel.innerHTML = `
    <div class="teacher-desk-wrap">
      <div class="teacher-desk-top">
        <div class="teacher-nameplate">INSTRUCTOR DESK · ACCOUNT SUMMARY</div>
        <div class="teacher-stats-row">
          ${tdStat('Active Trades', activeCount + ' / ' + sims.length)}
          ${tdStat('Total Trades', totalTrades)}
          ${tdStat('Avg Win Rate', avgWr != null ? avgWr.toFixed(1) + '%' : '—')}
        </div>
        <div class="att-banner" style="margin-top:6px;display:flex;gap:6px">
          <span class="att-pill active-pill">${activeCount} active</span>
          <span class="att-pill">${sims.length - activeCount} idle</span>
        </div>
      </div>
    </div>
  `;
}

function tdStat(label, value, cls = '') {
  return `<div class="td-stat">
    <div class="td-stat-label">${label}</div>
    <div class="td-stat-value ${cls}">${value}</div>
  </div>`;
}

// ─────────────────────────────────────────────── RECENT TRADES PANEL
let _rtAllTrades = [];
let _rtSims = [];
let _rtSymbols = [];
let _rtExpanded = false;
const RT_DEFAULT_LIMIT = 10;

let _rtPanelOpen = false;

async function fetchRecentTrades() {
  const panel = document.getElementById('recent-trades-panel');
  if (!panel) return;
  try {
    const r = await fetch('/api/trades/recent?limit=200');
    if (!r.ok) return;
    const data = await r.json();
    _rtAllTrades = data.trades || [];
    _rtSims      = data.sims    || [];
    _rtSymbols   = data.symbols || [];
    // Update toggle header meta
    const openCnt   = _rtAllTrades.filter(t => t.status === 'open').length;
    const closedCnt = _rtAllTrades.filter(t => t.status === 'closed').length;
    const metaEl = document.getElementById('rt-toggle-meta');
    if (metaEl) metaEl.textContent = `${openCnt} open · ${closedCnt} closed`;
    if (_rtPanelOpen) renderRecentTrades(panel);
  } catch(e) {}
}

function toggleRtPanel() {
  _rtPanelOpen = !_rtPanelOpen;
  const body    = document.getElementById('rt-panel-body');
  const chevron = document.getElementById('rt-toggle-chevron');
  if (body)    body.classList.toggle('open', _rtPanelOpen);
  if (chevron) chevron.textContent = _rtPanelOpen ? '▼' : '▶';
  if (_rtPanelOpen) renderRecentTrades(document.getElementById('recent-trades-panel'));
}

function _rtFilteredTrades() {
  const simF   = (document.getElementById('rt-f-sim')   || {}).value || '';
  const symF   = (document.getElementById('rt-f-sym')   || {}).value || '';
  const _maxEPRaw = ((document.getElementById('rt-f-maxep') || {}).value || '').trim();
  const maxEP = _maxEPRaw !== '' && !isNaN(parseFloat(_maxEPRaw)) ? parseFloat(_maxEPRaw) : null;
  const _minEPRaw = ((document.getElementById('rt-f-minep') || {}).value || '').trim();
  const minEP = _minEPRaw !== '' && !isNaN(parseFloat(_minEPRaw)) ? parseFloat(_minEPRaw) : null;
  return _rtAllTrades.filter(t => {
    if (simF && t.sim_id !== simF) return false;
    if (symF && t.symbol !== symF) return false;
    if (maxEP != null && t.entry_price != null && parseFloat(t.entry_price) > maxEP) return false;
    if (minEP != null && t.entry_price != null && parseFloat(t.entry_price) < minEP) return false;
    return true;
  });
}

function applyRtFilter() {
  _rtExpanded = false;
  renderRecentTrades(document.getElementById('recent-trades-panel'));
}

function toggleRtShowMore() {
  _rtExpanded = !_rtExpanded;
  renderRecentTrades(document.getElementById('recent-trades-panel'));
}

function renderRecentTrades(panel) {
  if (!panel) return;
  const filtered = _rtFilteredTrades();
  const limit    = _rtExpanded ? filtered.length : RT_DEFAULT_LIMIT;
  const visible  = filtered.slice(0, limit);
  const openCnt  = filtered.filter(t => t.status === 'open').length;

  const simOpts = _rtSims.map(s => `<option value="${s}">${s}</option>`).join('');
  const symOpts = _rtSymbols.map(s => `<option value="${s}">${s}</option>`).join('');

  if (!filtered.length) {
    panel.innerHTML = `<div class="rt-panel">
      <div class="rt-header-row">
        <div class="rt-title">RECENT TRADES <span class="rt-subtitle">no results</span></div>
        ${_rtFilterBar(simOpts, symOpts)}
      </div></div>`;
    return;
  }

  const rows = visible.map((t, idx) => {
    const pnl     = t.pnl != null ? parseFloat(t.pnl) : null;
    const pnlPct  = t.pnl_pct != null ? (parseFloat(t.pnl_pct) * 100).toFixed(1) : null;
    const pnlCls  = pnl > 0 ? 'pos' : pnl < 0 ? 'neg' : '';
    const pnlSign = pnl > 0 ? '+' : '';
    const pnlStr  = pnl != null ? pnlSign + '$' + fmt2(Math.abs(pnl)) : '—';
    const dir     = (t.direction || '').toUpperCase();
    const ct      = (t.contract_type || '').toUpperCase();
    const ctShort = ct === 'CALL' ? 'C' : ct === 'PUT' ? 'P' : ct.charAt(0) || '?';
    const strike  = t.strike != null ? '$' + parseFloat(t.strike).toFixed(0) : '—';
    const expiry  = t.expiry ? t.expiry.slice(5) : '—';
    const entryP  = t.entry_price != null ? '$' + fmt4(t.entry_price) : '—';
    const exitP   = t.exit_price  != null ? '$' + fmt4(t.exit_price)  : '—';
    const slStr   = t.sl_price != null ? '$' + fmt4(t.sl_price) : '—';
    const tpStr   = t.tp_price != null ? '$' + fmt4(t.tp_price) : '—';
    const isOpen  = t.status === 'open';
    const exitTime  = t.exit_time  ? fmtTime(t.exit_time)  : isOpen ? '<span class="rt-open-badge">OPEN</span>' : '—';
    const entryTime = t.entry_time ? fmtTime(t.entry_time) : '—';
    const holdStr = (() => {
      if (!t.entry_time) return null;
      const end = t.exit_time ? new Date(t.exit_time) : new Date();
      const secs = Math.max(0, Math.round((end - new Date(t.entry_time)) / 1000));
      if (secs < 60) return `${secs}s`;
      const m = Math.floor(secs / 60), h = Math.floor(m / 60), d = Math.floor(h / 24);
      if (d > 0) return `${d}d ${h % 24}h`;
      if (h > 0) return `${h}h ${m % 60}m`;
      return `${m}m`;
    })();
    const expandId  = `rt-expand-${idx}`;
    const rowCls    = isOpen ? 'rt-row rt-row-open' : `rt-row ${pnlCls}`;
    return `
    <tr class="${rowCls}" onclick="toggleRtExpand('${expandId}', this)">
      <td class="rt-sim">${t.sim_id}</td>
      <td><span class="sym-badge sym-${t.symbol}">${t.symbol}</span></td>
      <td><span class="dir-badge ${dir}">${dir || '—'}</span></td>
      <td class="rt-contract">${strike} ${ctShort} ${expiry}</td>
      <td class="rt-price">${entryP}</td>
      <td class="rt-price">${exitP}</td>
      <td class="rt-pnl ${pnlCls}">${isOpen ? '<span style="color:var(--dim);font-style:italic">open</span>' : pnlStr}</td>
      <td class="rt-time">${exitTime}</td>
      <td class="rt-chevron">▶</td>
    </tr>
    <tr class="rt-expand-row hidden" id="${expandId}">
      <td colspan="9">
        <div class="rt-expand-grid">
          <div class="rt-expand-item"><span class="rt-el">SL</span><span class="rt-ev neg">${slStr}</span></div>
          <div class="rt-expand-item"><span class="rt-el">TP</span><span class="rt-ev pos">${tpStr}</span></div>
          ${pnlPct != null ? `<div class="rt-expand-item"><span class="rt-el">P&L%</span><span class="rt-ev ${pnlCls}">${pnlSign}${pnlPct}%</span></div>` : ''}
          <div class="rt-expand-item"><span class="rt-el">Entry Time</span><span class="rt-ev">${entryTime}</span></div>
          ${holdStr ? `<div class="rt-expand-item"><span class="rt-el">Held</span><span class="rt-ev">${holdStr}</span></div>` : ''}
          <div class="rt-expand-item"><span class="rt-el">Qty</span><span class="rt-ev">${t.qty ?? '—'}</span></div>
          ${t.regime ? `<div class="rt-expand-item"><span class="rt-el">Regime</span><span class="rt-ev">${t.regime}</span></div>` : ''}
          ${!isOpen ? `<div class="rt-expand-item"><span class="rt-el">Exit Reason</span><span class="rt-ev">${t.exit_reason || '—'}</span></div>` : ''}
          ${t.exit_context ? `<div class="rt-expand-item rt-expand-wide"><span class="rt-el">Detail</span><span class="rt-ev">${t.exit_context}</span></div>` : ''}
          ${t.signal_mode ? `<div class="rt-expand-item"><span class="rt-el">Strategy</span><span class="rt-ev">${t.signal_mode}</span></div>` : ''}
        </div>
      </td>
    </tr>`;
  }).join('');

  const remaining = filtered.length - visible.length;
  const showMoreBtn = filtered.length > RT_DEFAULT_LIMIT ? `
    <button class="rt-show-more-btn" onclick="toggleRtShowMore()">
      ${_rtExpanded ? '▲ Show less' : `▼ Show ${remaining} more`}
    </button>` : '';

  const subtitle = `${openCnt > 0 ? `${openCnt} open · ` : ''}${filtered.length} total`;

  panel.innerHTML = `
    <div class="rt-panel">
      <div class="rt-header-row">
        <div class="rt-title">RECENT TRADES <span class="rt-subtitle">${subtitle}</span></div>
        ${_rtFilterBar(simOpts, symOpts)}
      </div>
      <div class="rt-scroll">
        <table class="rt-table">
          <thead><tr>
            <th>Sim</th><th>Symbol</th><th>Dir</th><th>Contract</th>
            <th>Entry</th><th>Exit</th><th>P&L</th><th>Time</th><th></th>
          </tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
      ${showMoreBtn}
    </div>`;
}

function _rtFilterBar(simOpts, symOpts) {
  const curSim   = (document.getElementById('rt-f-sim')   || {}).value || '';
  const curSym   = (document.getElementById('rt-f-sym')   || {}).value || '';
  const curMinEP = (document.getElementById('rt-f-minep') || {}).value || '';
  const curMaxEP = (document.getElementById('rt-f-maxep') || {}).value || '';
  return `<div class="rt-filters">
    <select id="rt-f-sim" class="rt-filter-sel" onchange="applyRtFilter()">
      <option value="">All Sims</option>${simOpts}
    </select>
    <select id="rt-f-sym" class="rt-filter-sel" onchange="applyRtFilter()">
      <option value="">All Symbols</option>${symOpts}
    </select>
    <input id="rt-f-minep" type="text" inputmode="decimal" class="rt-filter-input" placeholder="Min entry $"
           value="${curMinEP}" onchange="applyRtFilter()" onkeydown="if(event.key==='Enter')applyRtFilter()"/>
    <input id="rt-f-maxep" type="text" inputmode="decimal" class="rt-filter-input" placeholder="Max entry $"
           value="${curMaxEP}" onchange="applyRtFilter()" onkeydown="if(event.key==='Enter')applyRtFilter()"/>
  </div>`;
}

function toggleRtExpand(expandId, rowEl) {
  const expandRow = document.getElementById(expandId);
  if (!expandRow) return;
  const isOpen = !expandRow.classList.contains('hidden');
  expandRow.classList.toggle('hidden', isOpen);
  const chevron = rowEl.querySelector('.rt-chevron');
  if (chevron) chevron.textContent = isOpen ? '▶' : '▼';
}

// ─────────────────────────────────────────────── CLASSROOM RENDERING
const COLS = 6; // desks per row (6×N layout)

function getPersonality(signalMode) {
  const m = (signalMode || '').toUpperCase();
  if (['MEAN_REVERSION','VWAP_REVERSION','ZSCORE_BOUNCE','FAILED_BREAKOUT_REVERSAL','EXTREME_EXTENSION_FADE'].includes(m)) return 'scholar';
  if (['BREAKOUT','ORB_BREAKOUT','AFTERNOON_BREAKOUT','OPENING_DRIVE'].includes(m)) return 'athlete';
  if (['TREND_PULLBACK','SWING_TREND','VWAP_CONTINUATION','TREND_RECLAIM'].includes(m)) return 'trend';
  return 'casual';
}

function buildBubbleText(sim) {
  if (sim.open_count > 0) return '📈 In trade!';
  if (sim.pnl_dollars > 50)  return '🎉 +$' + fmt2(sim.pnl_dollars);
  if (sim.pnl_dollars < -50) return '😬 -$' + fmt2(Math.abs(sim.pnl_dollars));
  if (sim.total_trades === 0) return '⏳ Waiting…';
  return '📋 ' + shortName(sim.signal_mode || '') ;
}

// Strategy grouping order + display names
const STRATEGY_ORDER = ['scholar', 'athlete', 'trend', 'casual'];
const STRATEGY_LABEL = {
  scholar:  'MEAN REVERSION',
  athlete:  'BREAKOUT',
  trend:    'TREND',
  casual:   'OPPORTUNITY',
};

function renderDesks(sims) {
  const grid    = document.getElementById('desks-grid');
  const countEl = document.getElementById('sim-count');
  if (!sims.length) {
    grid.innerHTML = '<div class="loading-msg">No students enrolled yet.</div>';
    return;
  }

  const activeCount = sims.filter(s => s.open_count > 0).length;
  if (countEl) countEl.textContent = `${sims.length} students · ${activeCount} active`;

  grid.innerHTML = '';

  // Separate SIM00 (live) from the rest
  const liveSim  = sims.find(s => s.sim_id === 'SIM00');
  const paperSims = sims.filter(s => s.sim_id !== 'SIM00');

  // Group paper sims by strategy personality
  const groups = {};
  STRATEGY_ORDER.forEach(k => { groups[k] = []; });
  paperSims.forEach(sim => {
    const p = getPersonality(sim.signal_mode);
    (groups[p] = groups[p] || []).push(sim);
  });
  STRATEGY_ORDER.forEach(k => {
    if (groups[k]) groups[k].sort((a, b) => b.pnl_dollars - a.pnl_dollars);
  });

  function appendRow(label, group, extraClass) {
    const labelEl = document.createElement('div');
    labelEl.className = 'strategy-label' + (extraClass ? ' ' + extraClass : '');
    labelEl.textContent = label;
    grid.appendChild(labelEl);

    const rowEl = document.createElement('div');
    rowEl.className = 'desk-row';
    group.forEach(sim => rowEl.appendChild(buildSeat(sim)));
    grid.appendChild(rowEl);
  }

  STRATEGY_ORDER.forEach(key => {
    const group = groups[key];
    if (!group || !group.length) return;
    appendRow(STRATEGY_LABEL[key] || key.toUpperCase(), group, '');
  });

  // Live sim at the bottom
  if (liveSim) appendRow('LIVE', [liveSim], 'strategy-label-live');
}

function buildSeat(sim) {
  const active      = sim.open_count > 0;
  const mood        = sim.pnl_dollars > 0 ? 'happy' : sim.pnl_dollars < 0 ? 'sad' : 'neutral';
  const colorIdx    = Math.max(0, parseInt(sim.sim_id.replace('SIM', ''), 10) - 1);
  const pnlSign     = sim.pnl_dollars > 0 ? '+' : '';
  const pnlClass    = sim.pnl_dollars > 0 ? 'profit' : sim.pnl_dollars < 0 ? 'loss' : 'neutral';
  const personality = getPersonality(sim.signal_mode);
  const bubbleText  = buildBubbleText(sim);

  const seat = document.createElement('div');
  const wasSelected = currentSimId === sim.sim_id;
  seat.className = 'seat' + (active ? ' active' : '') + (wasSelected ? ' selected' : '');
  seat.dataset.simId = sim.sim_id;
  seat.onclick = () => openDrawer(sim.sim_id);

  const notebookClass = 'notebook ' + (sim.pnl_dollars > 0 ? 'profit' : sim.pnl_dollars < 0 ? 'loss' : '');

  const streakBadge = (() => {
    const s = sim.streak;
    if (!s || s.count < 1) return '';
    const n = s.count;
    if (s.type === 'win') {
      const FLAME_COLORS = ['#ff2222','#ff6600','#ffdd00','#44ee44','#3399ff','#bb44ff','#111111','#888888','#ffffff'];
      const color = FLAME_COLORS[Math.min(n - 1, FLAME_COLORS.length - 1)];
      return `<div class="streak-badge" style="color:${color};text-shadow:0 0 6px ${color}88" title="${n}-win streak">🔥${n > 1 ? `<span class="streak-num">${n}</span>` : ''}</div>`;
    } else {
      const STORM_COLORS = ['#88aaff','#4477ff','#2255dd','#7733cc','#440088','#220044','#555555','#888888','#aaaaaa'];
      const color = STORM_COLORS[Math.min(n - 1, STORM_COLORS.length - 1)];
      return `<div class="streak-badge" style="color:${color};text-shadow:0 0 6px ${color}88" title="${n}-loss streak">⛈️${n > 1 ? `<span class="streak-num">${n}</span>` : ''}</div>`;
    }
  })();

  seat.innerHTML = `
    <div class="student-area">
      <div class="speech-bubble">${bubbleText}</div>
      ${studentSVG(sim.sim_id, mood, colorIdx, personality, active)}
    </div>
    ${streakBadge}
    <div class="seat-info">
      <div class="seat-info-row">
        <span class="desk-id">${sim.sim_id}</span>
        <span class="desk-pnl ${pnlClass}">${pnlSign}$${fmt2(sim.pnl_dollars)}</span>
      </div>
      <div class="desk-footer">${shortName(sim.signal_mode || '')}</div>
      ${sim.symbols && sim.symbols.length ? `<div class="desk-symbols">${sim.symbols.join(' · ')}</div>` : ''}
    </div>
  `;
  return seat;
}

// ─────────────────────────────────────────────── STUDENT SVG (classroom character)
const SHIRT_COLORS = [
  '#4a7ac8','#7a5ab8','#4a9860','#c8882a','#b84a78',
  '#2a8898','#b85a2a','#5a882a','#b83a3a','#2a6898',
  '#988a2a','#5a4ab8','#2a8870','#884858','#3a5898',
  '#988050','#3a6870','#907830','#a83a68','#3a2e98',
  '#3a6888','#7a8830','#783a68',
];
const SKIN_TONES = ['#f9c990','#f0a870','#d08848','#b87030','#8a5028'];
const HAIR_COLORS = ['#1a1008','#5a3010','#c89030','#c04020','#707070','#101010'];

function studentSVG(simId, mood, idx, personality = 'casual', active = false) {
  const shirt = SHIRT_COLORS[idx % SHIRT_COLORS.length];
  const skin  = SKIN_TONES[(idx * 3) % SKIN_TONES.length];
  const hair  = HAIR_COLORS[(idx * 2) % HAIR_COLORS.length];

  // Laptop screen color reflects mood
  const screenBg = mood === 'happy' ? '#0e3a1a' : mood === 'sad' ? '#3a0e0e' : '#0e1e3a';
  const screenFg = mood === 'happy' ? '#44dd44' : mood === 'sad' ? '#dd4444' : '#4488dd';
  const screenGlow = active ? `<rect x="16" y="43" width="16" height="9" fill="${screenFg}" opacity="0.15"/>` : '';

  // Face expression
  let eyes = '', mouth = '';
  if (mood === 'happy') {
    // ^^ style happy eyes + smile
    eyes = `
      <path d="M19,19 Q20,17 21,19" fill="none" stroke="#333" stroke-width="1.2" stroke-linecap="round"/>
      <path d="M27,19 Q28,17 29,19" fill="none" stroke="#333" stroke-width="1.2" stroke-linecap="round"/>`;
    mouth = `<path d="M21,23 Q24,26 27,23" fill="none" stroke="#333" stroke-width="1.2" stroke-linecap="round"/>`;
  } else if (mood === 'sad') {
    // sad droopy eyes + frown
    eyes = `
      <path d="M19,18 Q20,20 21,18" fill="none" stroke="#333" stroke-width="1.2" stroke-linecap="round"/>
      <path d="M27,18 Q28,20 29,18" fill="none" stroke="#333" stroke-width="1.2" stroke-linecap="round"/>`;
    mouth = `<path d="M21,25 Q24,22 27,25" fill="none" stroke="#333" stroke-width="1.2" stroke-linecap="round"/>`;
  } else {
    // neutral dots + flat mouth
    eyes = `
      <circle cx="20" cy="19" r="1.2" fill="#333"/>
      <circle cx="28" cy="19" r="1.2" fill="#333"/>`;
    mouth = `<line x1="21" y1="24" x2="27" y2="24" stroke="#333" stroke-width="1.2" stroke-linecap="round"/>`;
  }

  // Front-facing personality accessories
  let accessory = '';
  if (personality === 'scholar') {
    // Glasses on face
    accessory = `
      <rect x="17" y="17.5" width="6" height="4" rx="1.5" fill="none" stroke="#444" stroke-width="1"/>
      <rect x="25" y="17.5" width="6" height="4" rx="1.5" fill="none" stroke="#444" stroke-width="1"/>
      <line x1="23" y1="19.5" x2="25" y2="19.5" stroke="#444" stroke-width="1"/>
      <line x1="17" y1="19.5" x2="15" y2="20" stroke="#444" stroke-width="0.8"/>
      <line x1="31" y1="19.5" x2="33" y2="20" stroke="#444" stroke-width="0.8"/>`;
  } else if (personality === 'athlete') {
    // Headband across forehead
    accessory = `
      <rect x="11" y="12" width="26" height="4" rx="2" fill="#e03030" opacity="0.9"/>
      <rect x="11" y="13" width="26" height="1.5" fill="rgba(255,255,255,0.2)" rx="0.5"/>`;
  } else if (personality === 'trend') {
    // Tie on chest
    accessory = `
      <polygon points="24,33 22,37 24,43 26,37" fill="#c09010" opacity="0.9"/>
      <rect x="22" y="32" width="4" height="3" rx="0.5" fill="#e0b020" opacity="0.85"/>`;
  }

  // Front-facing character: background → chair back → body → desk → face
  return `<svg viewBox="0 0 48 64" width="72" height="96" xmlns="http://www.w3.org/2000/svg" shape-rendering="crispEdges">

  <!-- ── CHAIR BACK ── -->
  <rect x="6"  y="28" width="36" height="6"  rx="3" fill="#6b3a12"/>
  <rect x="8"  y="29" width="32" height="4"  rx="2" fill="#8a4e1c"/>

  <!-- ── BODY / TORSO ── -->
  <!-- Neck -->
  <rect x="20" y="26" width="8" height="6" rx="2" fill="${skin}"/>
  <!-- Shirt body -->
  <rect x="9"  y="31" width="30" height="16" rx="3" fill="${shirt}"/>
  <!-- Collar V -->
  <polygon points="24,31 21,34 27,34" fill="${skin}" opacity="0.7"/>
  <!-- Left arm -->
  <rect x="2"  y="32" width="9" height="12" rx="4" fill="${shirt}"/>
  <rect x="1"  y="42" width="8" height="5"  rx="3" fill="${skin}"/>
  <!-- Right arm -->
  <rect x="37" y="32" width="9" height="12" rx="4" fill="${shirt}"/>
  <rect x="39" y="42" width="8" height="5"  rx="3" fill="${skin}"/>

  <!-- Personality accessory on body (tie) -->
  ${personality === 'trend' ? accessory : ''}

  <!-- ── DESK (in front of body, lower half) ── -->
  <!-- Desk top surface -->
  <rect x="0"  y="41" width="48" height="10" fill="#c88030"/>
  <line x1="0" y1="46" x2="48" y2="46" stroke="#a86820" stroke-width="0.6" opacity="0.4"/>
  <!-- Desk front edge (3D) -->
  <rect x="0"  y="51" width="48" height="5"  fill="#7a3e0c"/>
  <rect x="0"  y="55" width="48" height="1"  fill="rgba(0,0,0,0.2)"/>

  <!-- Laptop on desk -->
  <!-- Laptop base/keyboard -->
  <rect x="13" y="44" width="22" height="6" rx="1" fill="#2a2a3a"/>
  <rect x="14" y="45" width="20" height="4" rx="0.5" fill="#333345"/>
  <!-- Keys hint -->
  <rect x="15" y="46" width="4"  height="1" rx="0.3" fill="#4a4a60" opacity="0.9"/>
  <rect x="21" y="46" width="6"  height="1" rx="0.3" fill="#4a4a60" opacity="0.9"/>
  <rect x="29" y="46" width="4"  height="1" rx="0.3" fill="#4a4a60" opacity="0.9"/>
  <!-- Laptop lid (open, tilted back) -->
  <rect x="13" y="36" width="22" height="10" rx="1" fill="#1e1e2e"/>
  <rect x="14" y="37" width="20" height="8"  fill="${screenBg}"/>
  ${screenGlow}
  <!-- Screen content lines -->
  <rect x="16" y="38" width="10" height="1.5" fill="${screenFg}" opacity="0.75"/>
  <rect x="16" y="40" width="14" height="1.5" fill="${screenFg}" opacity="0.55"/>
  <rect x="16" y="42" width="7"  height="1.5" fill="${screenFg}" opacity="0.40"/>
  <!-- Laptop hinge line -->
  <rect x="13" y="45" width="22" height="1" fill="#111120" opacity="0.5"/>

  <!-- Papers left of desk -->
  <rect x="1"  y="42" width="10" height="8" rx="0.5" fill="#f0e8d4"/>
  <rect x="2"  y="44" width="8"  height="1" fill="rgba(140,120,80,0.4)"/>
  <rect x="2"  y="46" width="6"  height="1" fill="rgba(140,120,80,0.4)"/>
  <rect x="2"  y="48" width="7"  height="1" fill="rgba(140,120,80,0.4)"/>

  <!-- Mug right of desk -->
  <rect x="38" y="43" width="7" height="7" rx="1" fill="#bb6030"/>
  <rect x="38" y="43" width="7" height="2" rx="1" fill="#dd7040"/>
  <path d="M45,46 Q48,46 48,48 Q48,50 45,50" fill="none" stroke="#bb6030" stroke-width="1.3"/>

  <!-- ── HEAD ── -->
  <!-- Hair (top + sides) -->
  <rect x="11" y="5"  width="26" height="16" rx="6" fill="${hair}"/>
  <!-- Hair top volume -->
  <ellipse cx="24" cy="6" rx="11" ry="5" fill="${hair}"/>
  <!-- Face -->
  <rect x="13" y="10" width="22" height="18" rx="5" fill="${skin}"/>
  <!-- Ear left -->
  <rect x="10" y="14" width="4" height="6" rx="2" fill="${skin}"/>
  <!-- Ear right -->
  <rect x="34" y="14" width="4" height="6" rx="2" fill="${skin}"/>
  <!-- Hair shadow on forehead -->
  <rect x="13" y="10" width="22" height="4" rx="3" fill="${hair}" opacity="0.35"/>

  <!-- Eyes & mouth -->
  ${eyes}
  ${mouth}

  <!-- Nose -->
  <rect x="23" y="21" width="2" height="1" rx="0.5" fill="${skin}" opacity="0"/>
  <path d="M22,21 Q24,23 26,21" fill="none" stroke="rgba(0,0,0,0.15)" stroke-width="0.8"/>

  <!-- Personality accessory on head (glasses or headband) -->
  ${personality !== 'trend' ? accessory : ''}

  <!-- ── SHADOW ── -->
  <ellipse cx="24" cy="62" rx="18" ry="2" fill="rgba(0,0,0,0.15)"/>
</svg>`;
}

function shortName(mode) {
  const map = {
    MEAN_REVERSION: 'Mean Rev',
    BREAKOUT: 'Breakout',
    TREND_PULLBACK: 'Trend PB',
    SWING_TREND: 'Swing',
    OPPORTUNITY: 'Oppty',
    ORB_BREAKOUT: 'ORB',
    VWAP_REVERSION: 'VWAP Rev',
    ZSCORE_BOUNCE: 'ZScore',
    FAILED_BREAKOUT_REVERSAL: 'Fail-Rev',
    VWAP_CONTINUATION: 'VWAP Cont',
    OPENING_DRIVE: 'Open Dr',
    AFTERNOON_BREAKOUT: 'PM Break',
    TREND_RECLAIM: 'Reclaim',
    EXTREME_EXTENSION_FADE: 'Ext Fade',
  };
  return map[mode] || mode.replace(/_/g, ' ').toLowerCase();
}

// ─────────────────────────────────────────────── MULTI-SYMBOL CHARTS

// Convert any timestamp to a "fake UTC" epoch whose UTC wall-clock values equal ET (America/New_York).
// ApexCharts renders x-axis in UTC mode, so labels show the correct ET hour:minute.
function toETMs(t) {
  const d = new Date(typeof t === 'number' ? t : t);
  const fmt = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  });
  const p = {};
  fmt.formatToParts(d).forEach(({ type, value }) => { p[type] = value; });
  const h = p.hour === '24' ? 0 : +p.hour;
  return Date.UTC(+p.year, +p.month - 1, +p.day, h, +p.minute, +p.second);
}

function _makeApexOptions(sym, height) {
  return {
    chart: {
      type: 'line',
      height,
      background: 'transparent',
      toolbar: { show: false },
      animations: { enabled: false },
      foreColor: '#99bb99',
      sparkline: { enabled: false },
    },
    series: [
      { name: 'price',      type: 'candlestick', data: [] },
      { name: 'pred_cur',   type: 'line',        data: [] },
      { name: 'pred_prev',  type: 'line',        data: [] },
    ],
    stroke: { width: [1, 1.5, 1.5], dashArray: [0, 4, 4], curve: 'straight' },
    colors: ['transparent', '#4499ff', '#aa66ff'],
    xaxis: {
      type: 'datetime',
      labels: {
        datetimeUTC: true,
        style: { colors: '#88aa88', fontSize: '9px' },
        datetimeFormatter: { hour: 'HH:mm', minute: 'HH:mm' },
      },
      axisBorder: { color: 'rgba(160,210,140,0.15)' },
      axisTicks:  { color: 'rgba(160,210,140,0.15)' },
    },
    yaxis: {
      tooltip: { enabled: false },
      labels: {
        style: { colors: '#88aa88', fontSize: '9px' },
        formatter: v => v ? v.toFixed(2) : '',
      },
    },
    grid: {
      borderColor: 'rgba(140,200,120,0.1)',
      strokeDashArray: 4,
      padding: { left: 4, right: 4, top: 0, bottom: 0 },
    },
    plotOptions: {
      candlestick: {
        colors: { upward: '#88ee88', downward: '#ee8888' },
        wick: { useFillColor: true },
      },
    },
    markers: {
      size: [0, 5, 5],
      strokeWidth: 0,
      hover: { size: 6 },
    },
    legend: { show: false },
    tooltip: {
      theme: 'dark',
      x: { format: 'HH:mm' },
      shared: false,
      custom: ({ seriesIndex, dataPointIndex, w }) => {
        if (seriesIndex === 0) return ''; // let ApexCharts default handle candles
        const s = w.config.series[seriesIndex];
        const pt = s.data[dataPointIndex];
        if (!pt) return '';
        const label = seriesIndex === 1 ? 'Pred (cur)' : 'Pred (prev)';
        const color = seriesIndex === 1 ? '#4499ff' : '#aa66ff';
        return `<div style="padding:4px 8px;font-size:10px;color:#fff">${label}: <b>$${parseFloat(pt.y).toFixed(2)}</b></div>`;
      },
    },
  };
}

function focusSymbol(sym) {
  if (_focusedSym === sym) { unfocusSymbol(); return; }
  _focusedSym = sym;
  const grid = document.getElementById('symbol-charts-grid');
  if (grid) grid.classList.add('sym-focused-mode');
  document.querySelectorAll('.sym-chart-card').forEach(card => {
    card.classList.toggle('sym-focused', card.id === `sym-card-${sym}`);
  });
  const allBtn = document.getElementById('sym-all-btn');
  if (allBtn) allBtn.classList.remove('hidden');
  const titleEl = document.getElementById('chalk-overview-title');
  if (titleEl) titleEl.textContent = `${sym} · 1-MIN · LAST 60 BARS`;
  if (symbolCharts[sym]) symbolCharts[sym].updateOptions({ chart: { height: FOCUSED_CHART_H } }, false, false);
}

function unfocusSymbol() {
  const prev = _focusedSym;
  _focusedSym = null;
  const grid = document.getElementById('symbol-charts-grid');
  if (grid) grid.classList.remove('sym-focused-mode');
  document.querySelectorAll('.sym-chart-card').forEach(card => card.classList.remove('sym-focused'));
  const allBtn = document.getElementById('sym-all-btn');
  if (allBtn) allBtn.classList.add('hidden');
  const titleEl = document.getElementById('chalk-overview-title');
  if (titleEl) titleEl.textContent = 'MARKET OVERVIEW · 1-MIN · LAST 60 BARS';
  const chartH = window.innerWidth <= 480 ? 90 : window.innerWidth <= 900 ? 110 : 130;
  if (prev && symbolCharts[prev]) symbolCharts[prev].updateOptions({ chart: { height: chartH } }, false, false);
}

async function initSymbolCharts() {
  // Load symbol registry
  let registry = {};
  try {
    const r = await fetch('/api/symbols');
    if (r.ok) registry = await r.json();
  } catch {}
  _symbolRegistryCache = registry;

  const symbols = Object.keys(registry);
  if (!symbols.length) {
    // Fallback: just SPY
    symbols.push('SPY');
    registry['SPY'] = {};
  }

  const grid = document.getElementById('symbol-charts-grid');
  if (!grid) return;
  grid.innerHTML = '';

  const chartH = window.innerWidth <= 480 ? 90 : window.innerWidth <= 900 ? 110 : 130;

  symbols.forEach(sym => {
    const card = document.createElement('div');
    card.className = 'sym-chart-card';
    card.id = `sym-card-${sym}`;
    card.innerHTML = `
      <div class="sym-card-header" onclick="focusSymbol('${sym}')">
        <span class="sym-badge sym-${sym} sym-card-name">${sym}</span>
        <span class="sym-card-price" id="sym-price-${sym}">—</span>
        <span class="sym-card-change" id="sym-change-${sym}"></span>
        <div class="sym-card-pred" id="sym-pred-${sym}"></div>
      </div>
      <div id="sym-apex-${sym}"></div>
    `;
    grid.appendChild(card);

    const chart = new ApexCharts(
      document.getElementById(`sym-apex-${sym}`),
      _makeApexOptions(sym, chartH)
    );
    chart.render();
    symbolCharts[sym] = chart;
  });
}

async function fetchChartAndPredictions() {
  const symbols = Object.keys(_symbolRegistryCache || {});
  if (!symbols.length) return;

  try {
    const fetches = symbols.flatMap(sym => [
      fetch(`/api/chart?symbol=${sym}&bars=60`).then(r => r.json()).catch(() => ({ candles: [], symbol: sym })),
      fetch(`/api/predictions?symbol=${sym}`).then(r => r.json()).catch(() => ({ predictions: [], latest: null })),
    ]);
    const results = await Promise.all(fetches);

    // Compute shared x-axis window in ET epoch-ms
    let commonStart = 0, commonEnd = 0;
    for (let i = 0; i < symbols.length; i++) {
      const candles = results[i * 2].candles || [];
      if (!candles.length) continue;
      const t0 = toETMs(candles[0].t);
      const t1 = toETMs(candles[candles.length - 1].t);
      commonStart = commonStart ? Math.max(commonStart, t0) : t0;
      commonEnd   = commonEnd   ? Math.max(commonEnd,  t1) : t1;
    }

    for (let i = 0; i < symbols.length; i++) {
      const sym      = symbols[i];
      const chartData = results[i * 2];
      const predData  = results[i * 2 + 1];
      _updateSymbolCard(sym, chartData.candles || [], predData.predictions || [], commonStart || undefined, commonEnd || undefined);
    }
  } catch (e) {
    console.warn('symbol chart fetch error', e);
  }
}

function _updateSymbolCard(sym, candles, preds, xMin, xMax) {
  const chart = symbolCharts[sym];
  if (!chart) return;

  const PRED_DUR = 30 * 60 * 1000; // 30 min in ms

  // Sort predictions newest-first; prev must be ≥30 min older than latest
  const sorted = [...(preds || [])].sort((a, b) => new Date(b.time) - new Date(a.time));
  const latestPred = sorted[0] || null;
  const THIRTY_MIN_MS = 30 * 60 * 1000;
  const prevPred = latestPred
    ? (sorted.find(p => new Date(latestPred.time) - new Date(p.time) >= THIRTY_MIN_MS) || null)
    : null;

  function predSegment(p) {
    if (!p) return [];
    const dir = (p.direction || '').toUpperCase();
    const priceVal = dir === 'BULLISH' ? p.high : dir === 'BEARISH' ? p.low : null;
    if (priceVal == null) return [];
    const t0 = toETMs(p.time);
    const t1 = t0 + PRED_DUR;
    const y = parseFloat(priceVal);
    return [{ x: t0, y }, { x: t1, y }];
  }

  const candleSeries = candles.map(c => ({ x: toETMs(c.t), y: [c.o, c.h, c.l, c.c] }));
  const curSeg  = predSegment(latestPred);
  const prevSeg = predSegment(prevPred);

  chart.updateSeries([
    { name: 'price',     type: 'candlestick', data: candleSeries },
    { name: 'pred_cur',  type: 'line',        data: curSeg },
    { name: 'pred_prev', type: 'line',        data: prevSeg },
  ], true);

  if (xMin && xMax) {
    chart.updateOptions({ xaxis: { min: xMin, max: xMax + 60000 } }, false, false);
  }

  if (candles.length) {
    const last  = candles[candles.length - 1];
    const first = candles[0];
    const change    = last.c - first.o;
    const changePct = (change / first.o * 100).toFixed(2);
    const sign      = change >= 0 ? '+' : '';
    const priceEl  = document.getElementById(`sym-price-${sym}`);
    const changeEl = document.getElementById(`sym-change-${sym}`);
    if (priceEl)  { priceEl.textContent = `$${last.c.toFixed(2)}`; priceEl.style.color = change >= 0 ? 'var(--green)' : 'var(--red)'; }
    if (changeEl) { changeEl.textContent = `${sign}${changePct}%`; changeEl.style.color = change >= 0 ? 'var(--green)' : 'var(--red)'; }
  }

  // Update prediction badge (uses latest prediction)
  const predEl = document.getElementById(`sym-pred-${sym}`);
  if (latestPred) {
    const dir    = (latestPred.direction || '').toUpperCase();
    const conf   = latestPred.confidence != null ? `${(latestPred.confidence * 100).toFixed(0)}%` : '';
    const isBull = dir === 'BULLISH';
    const isBear = dir === 'BEARISH';
    const dirClass = isBull ? 'up' : isBear ? 'down' : 'flat';
    const arrow    = isBull ? '▲' : isBear ? '▼' : '—';
    const priceVal = isBull ? latestPred.high : isBear ? latestPred.low : null;
    const priceStr = priceVal != null ? ` $${parseFloat(priceVal).toFixed(2)}` : '';
    if (predEl) predEl.innerHTML = `<span class="sym-pred-dir ${dirClass}">${arrow}${priceStr} <span class="pred-conf">${conf}</span></span>`;
  } else {
    if (predEl) predEl.innerHTML = '';
  }
}

// ─────────────────────────────────────────────── DRAWER
async function openDrawer(simId) {
  // Mark previously selected, deselect old
  document.querySelectorAll('.seat.selected').forEach(el => el.classList.remove('selected'));
  const newSeat = document.querySelector(`.seat[data-sim-id="${simId}"]`);
  if (newSeat) newSeat.classList.add('selected');

  currentSimId = simId;

  const drawer   = document.getElementById('sim-drawer');
  const backdrop = document.getElementById('drawer-backdrop');
  drawer.classList.add('open');
  backdrop.classList.add('visible');

  // Reset to overview tab
  switchDrawerTab('overview', drawer.querySelector('.dtab'));

  // Loading state
  document.getElementById('drawer-sim-id').textContent = simId;
  document.getElementById('drawer-sim-name').textContent = '…';
  document.getElementById('drawer-signal-badge').innerHTML = '';
  document.getElementById('dpanel-overview').innerHTML =
    '<div style="padding:20px;text-align:center;color:var(--text-muted)">Loading…</div>';

  try {
    const r = await fetch(`/api/sim/${simId}`);
    const d = await r.json();
    populateDrawer(d);
  } catch (e) {
    document.getElementById('dpanel-overview').innerHTML =
      '<div style="padding:16px;color:var(--loss-text)">Failed to load sim data</div>';
  }
}

function closeDrawer() {
  document.getElementById('sim-drawer').classList.remove('open');
  document.getElementById('drawer-backdrop').classList.remove('visible');
  document.querySelectorAll('.seat.selected').forEach(el => el.classList.remove('selected'));
  if (perfChart) { perfChart.destroy(); perfChart = null; }
  currentSimId = null;
}

function switchDrawerTab(name, btn) {
  document.querySelectorAll('.drawer-panel').forEach(p => p.classList.add('hidden'));
  document.querySelectorAll('.dtab').forEach(b => b.classList.remove('active'));
  const panel = document.getElementById('dpanel-' + name);
  if (panel) panel.classList.remove('hidden');
  if (btn) btn.classList.add('active');
  // Render perf chart lazily when that tab is opened
  if (name === 'overview' && perfChart === null && currentSimId) {
    const bh = document._drawerBalanceHistory || [];
    setTimeout(() => renderPerfChart(bh, document._drawerBalanceStart), 50);
  }
}

function populateDrawer(d) {
  const stats   = d.stats || {};
  const profile = d.profile || {};

  const pnlCls  = stats.pnl_dollars > 0 ? 'pos' : stats.pnl_dollars < 0 ? 'neg' : '';
  const pnlSign = stats.pnl_dollars > 0 ? '+' : '';

  // Header
  document.getElementById('drawer-sim-id').textContent   = d.sim_id;
  document.getElementById('drawer-sim-name').textContent = d.name || '';
  document.getElementById('drawer-signal-badge').innerHTML =
    `<span class="signal-badge">${stats.signal_mode || '—'}</span>`;

  // Store for lazy chart render
  document._drawerBalanceHistory = d.balance_history || [];
  document._drawerBalanceStart   = stats.balance_start;

  // ── OVERVIEW TAB ──
  const ov = document.getElementById('dpanel-overview');
  // Per-symbol breakdown rows
  const symStats = stats.symbol_stats || {};
  const symRows = Object.entries(symStats).sort((a,b) => b[1].trades - a[1].trades).map(([sym, ss]) => {
    const pc = ss.pnl > 0 ? 'pos' : ss.pnl < 0 ? 'neg' : '';
    const sign = ss.pnl > 0 ? '+' : '';
    const wr = ss.win_rate != null ? ss.win_rate + '%' : '—';
    return `<tr class="sym-row">
      <td><span class="sym-badge sym-${sym}">${sym}</span></td>
      <td>${ss.trades}</td>
      <td class="${pc}">${sign}$${fmt2(ss.pnl)}</td>
      <td>${wr}</td>
    </tr>`;
  }).join('');
  const symTable = symRows ? `
    <div class="sym-breakdown">
      <div class="sym-breakdown-title">By Symbol</div>
      <table class="sym-table"><thead><tr><th>Symbol</th><th>Trades</th><th>P&L</th><th>WR</th></tr></thead>
      <tbody>${symRows}</tbody></table>
    </div>` : '';

  const sess = stats.session || {};
  const sessPnlCls = sess.pnl > 0 ? 'pos' : sess.pnl < 0 ? 'neg' : '';
  const sessPnlSign = sess.pnl > 0 ? '+' : '';
  const sessSection = `
    <div class="session-section">
      <div class="session-label">TODAY'S SESSION</div>
      <div class="drawer-stats-grid session-grid">
        ${dStat('Trades',   (sess.trades ?? 0) + (sess.open ? ` <span class="sess-open">(${sess.open} open)</span>` : ''), '', '')}
        ${dStat('P&L',      sess.trades > 0 || sess.open > 0 ? sessPnlSign + '$' + fmt2(sess.pnl) : '—', '', sessPnlCls)}
        ${dStat('Win Rate', sess.win_rate != null ? sess.win_rate + '%' : '—')}
        ${sess.best  != null ? dStat('Best',  '+$' + fmt2(sess.best),  '', 'pos') : ''}
        ${sess.worst != null ? dStat('Worst', '$'  + fmt2(sess.worst), '', sess.worst < 0 ? 'neg' : '') : ''}
      </div>
    </div>`;

  ov.innerHTML = `
    ${sessSection}
    <div class="session-label" style="padding:0 0 6px 2px;margin-top:12px">ALL TIME</div>
    <div class="drawer-stats-grid">
      ${dStat('Balance',   '$' + fmt2(stats.balance),          `Start: $${fmt2(stats.balance_start)}`)}
      ${dStat('P&L',       pnlSign + '$' + fmt2(stats.pnl_dollars), pnlSign + fmt2(stats.pnl_pct) + '%', pnlCls)}
      ${dStat('Trades',    stats.total_trades ?? '—')}
      ${dStat('Win Rate',  stats.win_rate != null ? stats.win_rate + '%' : '—')}
      ${dStat('Avg P&L',   stats.avg_pnl != null ? '$' + fmt2(stats.avg_pnl) : '—', '',
              stats.avg_pnl > 0 ? 'pos' : stats.avg_pnl < 0 ? 'neg' : '')}
      ${dStat('Max DD',    stats.max_drawdown_pct != null ? stats.max_drawdown_pct + '%' : '—', '', 'neg')}
      ${dStat('Best',      stats.best_trade != null ? '+$' + fmt2(stats.best_trade) : '—', '', 'pos')}
      ${dStat('Worst',     stats.worst_trade != null ? '$' + fmt2(stats.worst_trade) : '—', '',
              stats.worst_trade < 0 ? 'neg' : '')}
      ${dStat('Daily P&L', '$' + fmt2(stats.daily_loss), '', stats.daily_loss < 0 ? 'neg' : '')}
    </div>
    ${symTable}
    <div class="drawer-chart-wrap"><canvas id="perf-chart"></canvas></div>
  `;
  setTimeout(() => renderPerfChart(d.balance_history || [], stats.balance_start), 60);

  // ── ACTIVE TRADE TAB ──
  const openTrades = d.open_trades && d.open_trades.length ? d.open_trades
    : stats.open_trades && stats.open_trades.length ? stats.open_trades
    : stats.open_trade ? [stats.open_trade] : [];
  const tradePanel = document.getElementById('dpanel-trade');
  const _slPct = parseFloat(profile.stop_loss_pct) || 0;
  const _tpPct = parseFloat(profile.profit_target_pct) || 0;
  if (openTrades.length > 0) {
    tradePanel.innerHTML = openTrades.map(ot => {
      const op = parseOptionSymbol(ot.option_symbol);
      const symLabel = ot.symbol || (op ? op.ticker : null) || '—';
      const ep = ot.entry_price;
      const slPrice = (ep && _slPct) ? '$' + fmt4(ep * (1 - _slPct)) : '—';
      const tpPrice = (ep && _tpPct) ? '$' + fmt4(ep * (1 + _tpPct)) : '—';
      return `
      <div class="d-open-trade">
        <div class="d-open-trade-title">🟢 Open Position · ${symLabel}</div>
        <div class="d-open-trade-grid">
          ${dOtItem('Direction', `<span class="dir-badge ${ot.direction}">${ot.direction || '—'}</span>`)}
          ${dOtItem('Strike',  op ? '$' + op.strike : (ot.strike || '—'))}
          ${dOtItem('Type',    op ? op.type : '—')}
          ${dOtItem('Expiry',  op ? op.expiry : (ot.expiry || '—'))}
          ${dOtItem('Entry',   ep != null ? '$' + fmt4(ep) : '—')}
          ${dOtItem('SL',      slPrice, 'neg')}
          ${dOtItem('TP',      tpPrice, 'pos')}
          ${dOtItem('Qty',     ot.qty ?? '—')}
          ${dOtItem('Regime',  ot.regime || ot.regime_at_entry || '—')}
          ${dOtItem('Bucket',  ot.time_bucket || ot.time_of_day_bucket || '—')}
        </div>
      </div>`;
    }).join('');
  } else {
    tradePanel.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-muted);font-size:13px">No open position</div>';
  }

  // ── HISTORY TAB ──
  renderHistoryTab(d.sim_id, d.recent_trades || []);

  // ── PROFILE TAB ──
  const profPanel = document.getElementById('dpanel-profile');
  profPanel.innerHTML = `<div class="d-profile-grid">${
    Object.entries(profile)
      .filter(([k]) => !k.startsWith('_'))
      .map(([k, v]) => `
        <div class="d-profile-item">
          <span class="d-profile-key">${k}</span>
          <span class="d-profile-val">${Array.isArray(v) ? v.join(', ') : v ?? '—'}</span>
        </div>`)
      .join('')
  }</div>`;
}

// ─────────────────────────────────────────────── OPTION SYMBOL PARSER
// OCC format: TICKER + YYMMDD + C/P + 8-digit strike×1000
// e.g. SPY260317C00674000 → { ticker:'SPY', expiry:'Mar 17 \'26', type:'Call', strike:'674.00' }
function parseOptionSymbol(sym) {
  if (!sym) return null;
  const m = sym.match(/^([A-Z]{1,6})(\d{2})(\d{2})(\d{2})([CP])(\d{8})$/);
  if (!m) return null;
  const [, ticker, yy, mm, dd, typeChar, strikePad] = m;
  const strikeRaw = parseInt(strikePad, 10) / 1000;
  const strikeStr = strikeRaw % 1 === 0 ? String(strikeRaw) : strikeRaw.toFixed(2);
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  const monthStr = months[parseInt(mm, 10) - 1] || mm;
  return {
    ticker,
    type:   typeChar === 'C' ? 'Call' : 'Put',
    strike: strikeStr,
    expiry: `${monthStr} ${parseInt(dd)} '${yy}`,
    raw:    sym,
  };
}

function fmtOptionSymbol(sym) {
  const p = parseOptionSymbol(sym);
  if (!p) return sym || '—';
  return `${p.ticker} $${p.strike} ${p.type} · ${p.expiry}`;
}

// ─────────────────────────────────────────────── TRADE ACCORDION
function renderTradeAccordion(trades, container) {
  if (!trades.length) {
    container.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-muted);font-size:12px">No trades yet</div>';
    return;
  }
  container.innerHTML = trades.map((t, idx) => {
    const pnl     = t.pnl != null ? parseFloat(t.pnl) : null;
    const pnlPct  = t.pnl_pct != null ? (parseFloat(t.pnl_pct) * 100).toFixed(1) : null;
    const pnlCls  = pnl > 0 ? 'win' : pnl < 0 ? 'loss' : '';
    const pnlSign = pnl > 0 ? '+' : '';
    const pnlStr  = pnl != null ? pnlSign + '$' + fmt2(pnl) : '—';
    const pnlPctStr = pnlPct != null ? pnlSign + pnlPct + '%' : '';
    const dir     = (t.direction || '').toUpperCase();

    // Parse option symbol for human-readable display
    const opt = parseOptionSymbol(t.option_symbol);
    const symDisplay = opt ? `${opt.ticker} $${opt.strike} ${opt.type}` : (t.option_symbol || '—');
    const expiryDisplay = opt ? opt.expiry : (t.expiry || '—');

    return `
      <div class="trade-row-item ${pnlCls}" id="tr-${idx}">
        <div class="trade-row-summary" onclick="toggleTradeRow(${idx})">
          <span class="tr-time">${fmtTime(t.exit_time)}</span>
          <span class="tr-dir"><span class="dir-badge ${dir}">${dir || '—'}</span></span>
          <span class="tr-sym">${symDisplay}</span>
          <span class="tr-pnl ${pnlCls}">${pnlStr}${pnlPctStr ? ' <small style="opacity:.7">'+pnlPctStr+'</small>' : ''}</span>
          <span class="tr-toggle">▼</span>
        </div>
        <div class="trade-row-detail">
          <div class="trade-detail-grid">
            ${tdDetail('Exit Time',   fmtTime(t.exit_time))}
            ${tdDetail('Expiry',      expiryDisplay)}
            ${tdDetail('Strike',      opt ? '$' + opt.strike : (t.strike || '—'))}
            ${tdDetail('Type',        opt ? opt.type : dir)}
            ${tdDetail('Entry',       t.entry_price != null ? '$' + fmt4(t.entry_price) : '—')}
            ${tdDetail('Exit',        t.exit_price  != null ? '$' + fmt4(t.exit_price)  : '—')}
            ${tdDetail('P&L $',       pnlStr,  pnlCls)}
            ${tdDetail('P&L %',       pnlPctStr || '—', pnlCls)}
            ${tdDetail('Bucket',      t.time_bucket || '—')}
            ${tdDetail('Exit Reason', t.exit_reason || '—')}
            ${t.regime ? tdDetail('Regime', t.regime) : ''}
            ${tdDetail('Raw Symbol',  t.option_symbol || '—')}
          </div>
        </div>
      </div>`;
  }).join('');
}

function toggleTradeRow(idx) {
  const el = document.getElementById('tr-' + idx);
  if (!el) return;
  el.classList.toggle('expanded');
}

function tdDetail(label, val, cls = '') {
  return `<div class="td-detail-item">
    <span class="td-detail-label">${label}</span>
    <span class="td-detail-val ${cls}">${val}</span>
  </div>`;
}

function renderPerfChart(balanceHistory, startBalance) {
  if (perfChart) { perfChart.destroy(); perfChart = null; }
  const canvas = document.getElementById('perf-chart');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');

  const labels = balanceHistory.map(p => fmtTime(p.time));
  const data   = balanceHistory.map(p => p.balance);

  if (data.length === 0) {
    // Draw "No data" text on canvas
    ctx.fillStyle = '#64748b';
    ctx.font = '13px system-ui';
    ctx.textAlign = 'center';
    ctx.fillText('No completed trades yet', ctx.canvas.width / 2, ctx.canvas.height / 2);
    return;
  }

  const allValues = [startBalance, ...data];
  const minVal = Math.min(...allValues);
  const maxVal = Math.max(...allValues);

  perfChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'Balance',
        data,
        borderColor: '#4f8ef7',
        backgroundColor: 'rgba(79,142,247,0.08)',
        borderWidth: 2,
        pointRadius: data.length > 30 ? 0 : 3,
        pointHoverRadius: 5,
        fill: true,
        tension: 0.3,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: ctx => '$' + ctx.parsed.y.toFixed(2),
          },
        },
      },
      scales: {
        x: {
          ticks: { color: '#64748b', font: { size: 10 }, maxTicksLimit: 8, maxRotation: 0 },
          grid: { color: '#1e2230' },
        },
        y: {
          ticks: { color: '#64748b', font: { size: 10 }, callback: v => '$' + v.toFixed(0) },
          grid: { color: '#1e2230' },
          suggestedMin: minVal * 0.998,
          suggestedMax: maxVal * 1.002,
        },
      },
    },
  });
}

// ─────────────────────────────────────────────── HELPERS
function dStat(label, value, sub = '', cls = '') {
  return `<div class="dstat">
    <div class="dstat-label">${label}</div>
    <div class="dstat-value ${cls}">${value}</div>
    ${sub ? `<div class="dstat-sub">${sub}</div>` : ''}
  </div>`;
}

function dOtItem(label, val, cls = '') {
  return `<div class="d-ot-item">
    <div class="d-ot-label">${label}</div>
    <div class="d-ot-val${cls ? ' ' + cls : ''}">${val}</div>
  </div>`;
}

// Legacy helpers (kept for any remaining references)
function statCard(label, value, sub, cls = '') {
  return dStat(label, value, sub, cls);
}
function perfStat(label, value, cls = '') {
  return `<div class="dperf-stat"><div class="dperf-stat-label">${label}</div><div class="dperf-stat-value ${cls}">${value}</div></div>`;
}
function otItem(label, val) { return dOtItem(label, val); }

function fmt2(n)  { return n != null ? parseFloat(n).toFixed(2) : '—'; }
function fmt4(n)  { return n != null ? parseFloat(n).toFixed(4) : '—'; }

function fmtTime(iso) {
  if (!iso) return '—';
  try {
    const d = new Date(iso);
    return d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });
  } catch { return iso.slice(11, 16) || iso; }
}

// Close drawer on Escape
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') closeDrawer();
});

// ─────────────────────────────────────────────── HISTORY TAB (enhanced)
let _histSimId = null;
let _histPage  = 1;
let _histTotal = 0;
let _histFilters = { direction: 'ALL', result: 'all', date: '' };
let _selectedTradeId = null;
let _symbolRegistry = {};

async function _loadSymbolRegistry() {
  if (Object.keys(_symbolRegistry).length > 0) return _symbolRegistry;
  try {
    const r = await fetch('/api/symbols');
    if (r.ok) _symbolRegistry = await r.json();
  } catch {}
  return _symbolRegistry;
}

async function renderHistoryTab(simId, seedTrades) {
  _histSimId = simId;
  _histPage  = 1;
  const histPanel = document.getElementById('dpanel-history');

  // Load symbol registry to populate dropdown
  const syms = await _loadSymbolRegistry();
  const symOptions = Object.keys(syms).map(s =>
    `<option value="${s}">${s}</option>`).join('');

  histPanel.innerHTML = `
    <div class="history-header">
      <span class="history-title">TRADE LOG</span>
      <span class="history-count" id="hist-count">loading…</span>
    </div>
    <div class="hist-filters">
      <input  type="date" id="hist-date" class="hist-filter-input"
              onchange="applyHistFilter()" title="Filter by entry date"/>
      <select id="hist-dir" class="hist-filter-select" onchange="applyHistFilter()">
        <option value="ALL">All Directions</option>
        <option value="BULLISH">Calls</option>
        <option value="BEARISH">Puts</option>
      </select>
      <select id="hist-result" class="hist-filter-select" onchange="applyHistFilter()">
        <option value="all">All Results</option>
        <option value="win">Wins</option>
        <option value="loss">Losses</option>
      </select>
      ${symOptions ? `<select id="hist-symbol" class="hist-filter-select" onchange="applyHistFilter()">
        <option value="">All Symbols</option>
        ${symOptions}
      </select>` : ''}
    </div>
    <div class="trade-accordion" id="trade-accordion"></div>
    <div class="hist-pagination" id="hist-pagination"></div>
  `;
  // Try to load from dedicated endpoint; fall back to seedTrades
  loadHistoryPage(1, seedTrades);
}

async function loadHistoryPage(page, seedTrades) {
  _histPage = page;
  if (!_histSimId) return;

  const dateVal = (document.getElementById('hist-date')   || {}).value || '';
  const dirVal  = (document.getElementById('hist-dir')    || {}).value || 'ALL';
  const resVal  = (document.getElementById('hist-result') || {}).value || 'all';
  const symVal  = (document.getElementById('hist-symbol') || {}).value || '';

  let trades = [], total = 0;
  try {
    let url = `/api/trades/${_histSimId}/history?page=${page}&per_page=50`;
    if (dateVal) url += `&date=${dateVal}`;
    if (dirVal  !== 'ALL')  url += `&direction=${dirVal}`;
    if (resVal  !== 'all')  url += `&result=${resVal}`;
    if (symVal)             url += `&symbol=${symVal}`;
    const r = await fetch(url);
    if (r.ok) {
      const data = await r.json();
      trades = data.trades || [];
      total  = data.total  || 0;
    } else if (seedTrades && seedTrades.length) {
      trades = seedTrades; total = seedTrades.length;
    }
  } catch {
    trades = seedTrades || []; total = trades.length;
  }

  _histTotal = total;
  const countEl = document.getElementById('hist-count');
  if (countEl) countEl.textContent = `${total} trade${total !== 1 ? 's' : ''}`;

  const container = document.getElementById('trade-accordion');
  if (container) renderTradeAccordionWithDetails(trades, container, _histSimId);

  // Pagination
  const pgEl = document.getElementById('hist-pagination');
  if (pgEl) {
    const totalPages = Math.ceil(total / 50);
    pgEl.innerHTML = totalPages > 1 ? `
      <button class="hist-pg-btn" onclick="loadHistoryPage(${page-1})"
              ${page <= 1 ? 'disabled' : ''}>← Prev</button>
      <span class="hist-pg-label">Page ${page} / ${totalPages}</span>
      <button class="hist-pg-btn" onclick="loadHistoryPage(${page+1})"
              ${page >= totalPages ? 'disabled' : ''}>Next →</button>
    ` : '';
  }
}

function applyHistFilter() { loadHistoryPage(1); }

// ─────────────────────────────────────────────── TRADE CARDS WITH DETAILS BUTTON
function renderTradeAccordionWithDetails(trades, container, simId) {
  if (!trades.length) {
    container.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-muted);font-size:12px">No trades</div>';
    return;
  }
  let _lastDate = null;
  container.innerHTML = trades.map((t, idx) => {
    const pnl     = t.pnl != null ? parseFloat(t.pnl) : null;
    const pnlPct  = t.pnl_pct != null ? (parseFloat(t.pnl_pct) * 100).toFixed(1) : null;
    const pnlCls  = pnl > 0 ? 'win' : pnl < 0 ? 'loss' : '';
    const pnlSign = pnl > 0 ? '+' : '';
    const pnlStr  = pnl != null ? pnlSign + '$' + fmt2(Math.abs(pnl)) : '—';
    const dir     = (t.direction || '').toUpperCase();
    const opt     = parseOptionSymbol(t.option_symbol);
    const symDisp = opt ? `${opt.ticker} $${opt.strike} ${opt.type}` : (t.option_symbol || '—');
    const isSelected = t.trade_id && t.trade_id === _selectedTradeId;
    const undSym  = t.symbol || (opt ? opt.ticker : '');
    const symBadge = undSym ? `<span class="sym-badge sym-${undSym}">${undSym}</span>` : '';

    // Date separator
    const tsStr = t.exit_time || t.entry_time || '';
    const dateStr = tsStr.slice(0, 10);
    let dateSep = '';
    if (dateStr && dateStr !== _lastDate) {
      _lastDate = dateStr;
      const d = new Date(dateStr + 'T12:00:00');
      const label = isNaN(d) ? dateStr : d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
      dateSep = `<div class="tr-date-sep">${label}</div>`;
    }

    return dateSep + `
      <div class="trade-row-item ${pnlCls}${isSelected ? ' tr-selected' : ''}" id="tr-${idx}">
        <div class="trade-row-summary" onclick="toggleTradeRow(${idx})">
          <span class="tr-time">${fmtTime(t.exit_time || t.entry_time)}</span>
          <span class="tr-dir"><span class="dir-badge ${dir}">${dir || '—'}</span></span>
          ${symBadge}
          <span class="tr-sym">${symDisp}</span>
          <span class="tr-pnl ${pnlCls}">${pnl != null ? (pnlSign + '$' + fmt2(Math.abs(pnl))) : '—'}</span>
          ${t.grade ? `<span class="tr-grade grade-badge grade-${t.grade.toLowerCase()}">${t.grade}</span>` : '<span class="tr-grade"></span>'}
          <span class="tr-toggle">▼</span>
        </div>
        <div class="trade-row-detail">
          <div class="trade-detail-grid">
            ${tdDetail('Expiry',      opt ? opt.expiry : (t.expiry || '—'))}
            ${tdDetail('Strike',      opt ? '$' + opt.strike : (t.strike ? '$' + t.strike : '—'))}
            ${tdDetail('Type',        opt ? opt.type : (t.contract_type === 'C' ? 'Call' : t.contract_type === 'P' ? 'Put' : dir))}
            ${tdDetail('Entry',       t.entry_price != null ? '$' + fmt4(t.entry_price) : '—')}
            ${tdDetail('Exit',        t.exit_price  != null ? '$' + fmt4(t.exit_price)  : '—')}
            ${t.sl_price != null ? tdDetail('SL', '$' + fmt4(t.sl_price), 'neg') : ''}
            ${t.tp_price != null ? tdDetail('TP', '$' + fmt4(t.tp_price), 'pos') : ''}
            ${tdDetail('P&L $',       pnl != null ? (pnlSign + '$' + fmt2(Math.abs(pnl))) : '—',  pnlCls)}
            ${tdDetail('P&L %',       pnlPct != null ? (pnlSign + pnlPct + '%') : '—',            pnlCls)}
            ${tdDetail('Exit Reason', t.exit_reason || '—')}
            ${t.exit_context ? tdDetail('Detail', t.exit_context) : ''}
            ${t.regime ? tdDetail('Regime', t.regime) : ''}
          </div>
          ${t.trade_id ? `
          <button class="tr-details-btn${isSelected ? ' tr-details-btn-active' : ''}"
                  onclick="openTradeDetails('${simId}','${t.trade_id}',${idx},event)">
            ${isSelected ? '✓ Viewing' : '[Details]'}
          </button>` : ''}
        </div>
      </div>`;
  }).join('');
}

// ─────────────────────────────────────────────── CHALKBOARD TRADE ANALYSIS
async function openTradeDetails(simId, tradeId, rowIdx, e) {
  if (e) e.stopPropagation();
  _selectedTradeId = tradeId;

  // Re-render accordion to update selected highlight
  const container = document.getElementById('trade-accordion');
  if (container) {
    container.querySelectorAll('.trade-row-item').forEach((el, i) => {
      el.classList.toggle('tr-selected', i === rowIdx);
    });
    const btn = container.querySelector(`#tr-${rowIdx} .tr-details-btn`);
    if (btn) { btn.textContent = '✓ Viewing'; btn.classList.add('tr-details-btn-active'); }
  }

  // Switch chalkboard to trade analysis mode
  showTradeChart(simId, tradeId);

  // If on mobile, close drawer so chalkboard is visible
  if (window.innerWidth < 768) closeDrawer();
}

function showTradeChart(simId, tradeId) {
  document.getElementById('chalk-live-view').classList.add('hidden');
  document.getElementById('chalk-trade-view').classList.remove('hidden');

  const img     = document.getElementById('trade-chart-img');
  const loading = document.getElementById('trade-chart-loading');
  const errEl   = document.getElementById('trade-chart-error');
  const narBtn  = document.getElementById('narrate-btn');

  img.classList.add('hidden');
  errEl.classList.add('hidden');
  loading.classList.remove('hidden');

  // Store for narrate button
  document._currentTradeSimId = simId;
  document._currentTradeId    = tradeId;
  if (narBtn) narBtn.textContent = '✨ Analyze';

  // Update title
  const titleEl = document.getElementById('trade-chart-title');
  if (titleEl) titleEl.textContent = `${simId} · TRADE ANALYSIS`;

  const src = `/api/trades/${simId}/${encodeURIComponent(tradeId)}/chart?t=${Date.now()}`;
  img.onload  = () => { loading.classList.add('hidden'); img.classList.remove('hidden'); };
  img.onerror = () => { loading.classList.add('hidden'); errEl.classList.remove('hidden'); };
  img.src = src;

  // Check for cached narrative
  loadCachedNarrative(simId, tradeId);
}

function showLiveChart() {
  document.getElementById('chalk-live-view').classList.remove('hidden');
  document.getElementById('chalk-trade-view').classList.add('hidden');
  document.getElementById('narrative-panel').classList.add('hidden');
  _selectedTradeId = null;
}

async function loadCachedNarrative(simId, tradeId) {
  const panel = document.getElementById('narrative-panel');
  try {
    const r = await fetch(`/api/trades/${simId}/${encodeURIComponent(tradeId)}/narrative`);
    if (r.ok) {
      const narr = await r.json();
      renderNarrative(narr);
      const btn = document.getElementById('narrate-btn');
      if (btn) btn.textContent = '↻ Re-analyze';
    } else {
      panel.classList.remove('hidden');
      document.getElementById('narrative-loading').classList.add('hidden');
      document.getElementById('narrative-content').classList.add('hidden');
    }
  } catch { /* no narrative yet */ }
}

async function requestNarrative(force = false) {
  const simId   = document._currentTradeSimId;
  const tradeId = document._currentTradeId;
  if (!simId || !tradeId) return;

  const panel   = document.getElementById('narrative-panel');
  const loading = document.getElementById('narrative-loading');
  const content = document.getElementById('narrative-content');
  const btn     = document.getElementById('narrate-btn');

  panel.classList.remove('hidden');
  loading.classList.remove('hidden');
  content.classList.add('hidden');
  if (btn) { btn.disabled = true; btn.textContent = '…'; }

  try {
    const url = `/api/trades/${simId}/${encodeURIComponent(tradeId)}/narrate${force ? '?force=true' : ''}`;
    const r   = await fetch(url, { method: 'POST' });
    if (r.ok) {
      const narr = await r.json();
      renderNarrative(narr);
      // Re-fetch chart with new S/R lines
      if (force) showTradeChart(simId, tradeId);
      if (btn) { btn.disabled = false; btn.textContent = '↻ Re-analyze'; }
    } else {
      loading.classList.add('hidden');
      content.innerHTML = '<div style="padding:12px;color:var(--loss-text)">Analysis failed — check OPENAI_API_KEY.</div>';
      content.classList.remove('hidden');
      if (btn) { btn.disabled = false; btn.textContent = '✨ Analyze'; }
    }
  } catch(e) {
    loading.classList.add('hidden');
    if (btn) { btn.disabled = false; btn.textContent = '✨ Analyze'; }
  }
}

function renderNarrative(narr) {
  const loading = document.getElementById('narrative-loading');
  const content = document.getElementById('narrative-content');
  const panel   = document.getElementById('narrative-panel');

  const _unavailMsgs = [
    'Analysis unavailable — GPT service unreachable or API key not set.',
    'Analysis unavailable — OpenAI API key invalid or expired. Update OPENAI_API_KEY in .env',
  ];
  if (!narr || _unavailMsgs.includes(narr.entry_reasoning)) {
    loading.classList.add('hidden');
    const msg = (narr && narr.entry_reasoning && narr.entry_reasoning.includes('invalid or expired'))
      ? 'AI analysis unavailable — OpenAI API key invalid or expired.'
      : 'AI analysis unavailable — OPENAI_API_KEY not set in .env';
    content.innerHTML = `<div style="padding:12px;color:var(--loss-text);font-size:12px">${msg}</div>`;
    content.classList.remove('hidden');
    panel.classList.remove('hidden');
    return;
  }

  document.getElementById('narr-summary').textContent  = narr.strategy_summary || '';
  document.getElementById('narr-entry-text').textContent  = narr.entry_reasoning  || '—';
  document.getElementById('narr-exit-text').textContent   = narr.exit_reasoning   || '—';
  document.getElementById('narr-outcome-text').textContent = narr.outcome_analysis || '—';

  // Grade
  const gradeEl = document.getElementById('narr-grade');
  const grade   = narr.grade || '';
  const gradeCls = {A:'grade-a',B:'grade-b',C:'grade-c',D:'grade-d',F:'grade-f'}[grade] || '';
  gradeEl.innerHTML = grade ? `<span class="grade-badge ${gradeCls}">${grade}</span>` : '';

  // Tags
  const tagsEl = document.getElementById('narr-tags');
  const tags   = narr.tags || [];
  tagsEl.innerHTML = tags.map(t => `<span class="narr-tag">${t}</span>`).join('');

  loading.classList.add('hidden');
  content.classList.remove('hidden');
  panel.classList.remove('hidden');
}
