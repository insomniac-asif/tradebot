/* ═══════════════════════════════════════════════════════ SpyBot Dashboard */
'use strict';

const POLL_INTERVAL = 30000; // 30s
let apexChart = null;
let perfChart = null;
let simsCache = [];
let currentSimId = null;

// ─────────────────────────────────────────────── INIT
document.addEventListener('DOMContentLoaded', () => {
  initWhiteboard();
  startClock();
  refreshAll();
  setInterval(refreshAll, POLL_INTERVAL);
});

async function refreshAll() {
  await Promise.all([
    fetchStatus(),
    fetchSims(),
    fetchChartAndPredictions(),
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
          ${tdStat('Total Balance', '$' + fmt2(totalBal))}
          ${tdStat('Total P&L', pnlSign + '$' + fmt2(totalPnl), pnlClass)}
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
  // Restore selected class if this was previously selected
  const wasSelected = currentSimId === sim.sim_id;
  seat.className = 'seat' + (active ? ' active' : '') + (wasSelected ? ' selected' : '');
  seat.dataset.simId = sim.sim_id;
  seat.onclick = () => openDrawer(sim.sim_id);

  const notebookClass = 'notebook ' + (sim.pnl_dollars > 0 ? 'profit' : sim.pnl_dollars < 0 ? 'loss' : '');

  seat.innerHTML = `
    <div class="student-area">
      <div class="speech-bubble">${bubbleText}</div>
      ${studentSVG(sim.sim_id, mood, colorIdx, personality, active)}
    </div>
    <div class="seat-info">
      <div class="seat-info-row">
        <span class="desk-id">${sim.sim_id}</span>
        <span class="desk-pnl ${pnlClass}">${pnlSign}$${fmt2(sim.pnl_dollars)}</span>
      </div>
      <div class="desk-footer">${shortName(sim.signal_mode || '')}</div>
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

// ─────────────────────────────────────────────── WHITEBOARD CHART
function initWhiteboard() {
  const options = {
    chart: {
      type: 'candlestick',
      height: window.innerWidth <= 480 ? 160 : window.innerWidth <= 640 ? 200 : 260,
      background: 'transparent',
      toolbar: { show: false },
      animations: { enabled: false },
      foreColor: '#99bb99',
    },
    series: [{ data: [] }],
    xaxis: {
      type: 'datetime',
      labels: {
        style: { colors: '#88aa88', fontSize: '10px' },
        datetimeFormatter: { hour: 'HH:mm', minute: 'HH:mm' },
      },
      axisBorder: { color: 'rgba(160,210,140,0.2)' },
      axisTicks: { color: 'rgba(160,210,140,0.2)' },
    },
    yaxis: {
      tooltip: { enabled: true },
      labels: {
        style: { colors: '#88aa88', fontSize: '10px' },
        formatter: v => v ? v.toFixed(2) : '',
      },
    },
    grid: {
      borderColor: 'rgba(140,200,120,0.12)',
      strokeDashArray: 4,
    },
    plotOptions: {
      candlestick: {
        colors: { upward: '#88ee88', downward: '#ee8888' },
        wick: { useFillColor: true },
      },
    },
    tooltip: {
      theme: 'dark',
      x: { format: 'HH:mm' },
    },
  };
  apexChart = new ApexCharts(document.getElementById('apex-chart'), options);
  apexChart.render();
}

async function fetchChartAndPredictions() {
  try {
    const [chartRes, predRes] = await Promise.all([
      fetch('/api/chart?bars=60'),
      fetch('/api/predictions'),
    ]);
    const chartData = await chartRes.json();
    const predData  = await predRes.json();

    updateWhiteboard(chartData.candles || []);
    updatePredictionBadge(predData.latest);
  } catch (e) {
    console.warn('chart/pred fetch error', e);
  }
}

function updateWhiteboard(candles) {
  if (!candles.length) return;

  const series = candles.map(c => ({
    x: new Date(c.t).getTime(),
    y: [c.o, c.h, c.l, c.c],
  }));

  apexChart.updateSeries([{ data: series }], true);

  const last = candles[candles.length - 1];
  const first = candles[0];
  const priceEl = document.getElementById('chart-price');
  const rangeEl = document.getElementById('chart-range');

  if (last) {
    const change = last.c - first.o;
    const changePct = (change / first.o * 100).toFixed(2);
    const sign = change >= 0 ? '+' : '';
    priceEl.textContent = `$${last.c.toFixed(2)}`;
    priceEl.style.color = change >= 0 ? 'var(--green)' : 'var(--red)';
    rangeEl.textContent = `${sign}${change.toFixed(2)} (${sign}${changePct}%)  H: ${last.h.toFixed(2)}  L: ${last.l.toFixed(2)}`;
  }
}

function updatePredictionBadge(latest) {
  const dirEl  = document.getElementById('pred-dir');
  const confEl = document.getElementById('pred-conf');
  const metaEl = document.getElementById('pred-meta');
  const chkEl  = document.getElementById('pred-checked');

  if (!latest) {
    dirEl.textContent = '—';
    dirEl.className = 'pred-dir flat';
    confEl.textContent = '';
    metaEl.textContent = 'No prediction';
    return;
  }

  const dir  = (latest.direction || '').toUpperCase();
  const conf = latest.confidence != null ? `${(latest.confidence * 100).toFixed(0)}%` : '';
  const regime = latest.regime || '';
  const session = latest.session || '';
  const checked = latest.checked;
  const correct = latest.correct;

  dirEl.textContent = dir === 'UP' ? '▲ UP' : dir === 'DOWN' ? '▼ DOWN' : dir || '—';
  dirEl.className = 'pred-dir ' + (dir === 'UP' ? 'up' : dir === 'DOWN' ? 'down' : 'flat');
  confEl.textContent = conf;
  metaEl.textContent = [regime, session].filter(Boolean).join(' · ');

  if (checked) {
    chkEl.textContent = correct ? '✓ Correct' : '✗ Incorrect';
    chkEl.style.color = correct ? 'var(--green)' : 'var(--red)';
  } else {
    chkEl.textContent = 'Pending';
    chkEl.style.color = 'var(--muted)';
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
  ov.innerHTML = `
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
    <div class="drawer-chart-wrap"><canvas id="perf-chart"></canvas></div>
  `;
  setTimeout(() => renderPerfChart(d.balance_history || [], stats.balance_start), 60);

  // ── ACTIVE TRADE TAB ──
  const ot  = stats.open_trade;
  const tradePanel = document.getElementById('dpanel-trade');
  if (ot) {
    tradePanel.innerHTML = `
      <div class="d-open-trade">
        <div class="d-open-trade-title">🟢 Open Position</div>
        <div class="d-open-trade-grid">
          ${(()=>{const op=parseOptionSymbol(ot.option_symbol); return `
          ${dOtItem('Direction', `<span class="dir-badge ${ot.direction}">${ot.direction || '—'}</span>`)}
          ${dOtItem('Ticker',  op ? op.ticker : (ot.option_symbol || '—'))}
          ${dOtItem('Strike',  op ? '$' + op.strike : (ot.strike || '—'))}
          ${dOtItem('Type',    op ? op.type : '—')}
          ${dOtItem('Expiry',  op ? op.expiry : (ot.expiry || '—'))}
          ${dOtItem('Entry',   ot.entry_price != null ? '$' + fmt4(ot.entry_price) : '—')}
          ${dOtItem('Qty',     ot.qty ?? '—')}
          ${dOtItem('Regime',  ot.regime || '—')}
          ${dOtItem('Bucket',  ot.time_bucket || '—')}
          ${dOtItem('Score',   ot.structure_score ?? '—')}
          `;})()}
        </div>
      </div>
    `;
  } else {
    tradePanel.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-muted);font-size:13px">No open position</div>';
  }

  // ── HISTORY TAB ──
  const trades = d.recent_trades || [];
  const histPanel = document.getElementById('dpanel-history');
  histPanel.innerHTML = `
    <div class="history-header">
      <span class="history-title">TRADE LOG</span>
      <span class="history-count">${trades.length} trades</span>
    </div>
    <div class="trade-accordion" id="trade-accordion"></div>
  `;
  renderTradeAccordion(trades, document.getElementById('trade-accordion'));

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

function dOtItem(label, val) {
  return `<div class="d-ot-item">
    <div class="d-ot-label">${label}</div>
    <div class="d-ot-val">${val}</div>
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
