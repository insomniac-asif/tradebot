import os
import json
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def _extract_response_text(response) -> str | None:
    # Newer SDKs may expose text in different shapes depending on model/tooling.
    try:
        output_text = getattr(response, "output_text", None)
        if isinstance(output_text, str) and output_text.strip():
            return output_text.strip()
    except Exception:
        pass

    try:
        choices = getattr(response, "choices", None)
        if choices:
            message = getattr(choices[0], "message", None)
            content = getattr(message, "content", None)
            if isinstance(content, str) and content.strip():
                return content.strip()
            if isinstance(content, list):
                parts = []
                for item in content:
                    if isinstance(item, dict):
                        text = item.get("text")
                        if isinstance(text, str) and text.strip():
                            parts.append(text.strip())
                    else:
                        text = getattr(item, "text", None)
                        if isinstance(text, str) and text.strip():
                            parts.append(text.strip())
                joined = "\n".join(parts).strip()
                if joined:
                    return joined
    except Exception:
        pass

    return None


def ask_ai(
    question,
    market_summary,
    paper_stats,
    career_stats,
    last_trades,
    sim_context=None,
    prior_context=None,
):

    system_prompt = f"""
You are an options trading performance coach.

You do NOT hype trades.
You do NOT encourage gambling.
You analyze objectively.

Your job:
Evaluate SPY trading decisions using the user's real trading data.

Market Context:
{market_summary}

Paper Account Stats:
{paper_stats}

Career Stats:
{career_stats}

Recent Trades:
{last_trades}

SIM Context:
{sim_context or "None provided"}

Prior Conversation Context:
{prior_context or "None provided"}

When answering:
- Explain WHY
- Point out behavioral mistakes
- Mention overtrading, chasing, late entries if seen
- Prefer risk management over prediction
- If the question is about SIMs, use the SIM Context above.
- If SIM context exists, explicitly reference relevant SIM IDs and concrete metrics.
- Default to detailed support for every question (not one-liners).
- Use this structure with clear labels:
  Assessment:
  Evidence:
  Likely Causes:
  Next Actions:
- Keep the tone direct, practical, and specific to provided data.
- If data is insufficient, state exactly what is missing and what to check next.
- Avoid generic advice; tie recommendations to numbers, exits, risk settings, and regime context when available.
- Target 6-14 sentences total and keep output under ~1800 characters.
- End with one concrete follow-up question that can be asked with !askmore.
"""

    try:
        response = client.chat.completions.create(
            model="gpt-5-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": question}
            ],
            max_completion_tokens=900
        )
    except Exception:
        return None

    return _extract_response_text(response)
