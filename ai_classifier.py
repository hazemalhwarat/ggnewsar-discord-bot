"""
GGNewsAR Bot — AI second-opinion classifier (Gemini free tier, optional).

WHY THIS EXISTS
----------------
relevance.py (keyword-based, free, instant) is the FIRST and MAIN filter —
it resolves the vast majority of items with zero cost. But it has a real
ceiling: it can only recognize teams/players/tournaments that are already
in its hardcoded lists. A genuinely relevant story like "PlayTime bring in
Abed and Jabz for Games of the Future" will fail the keyword filter if it
comes from an untrusted bridge source, purely because "PlayTime", "Abed",
"Jabz" and "Games of the Future" aren't (and can never fully be) in a
manually maintained list — the global esports scene has thousands of
teams/players/tournaments and it keeps changing.

This module is ONLY a second opinion for that narrow "ambiguous" bucket:
items the keyword filter did NOT accept, but also did NOT hard-exclude
(i.e. not obviously a hardware deal / story-mode game / unrelated topic).
For those, one cheap Gemini call asks "does this look like real
competitive-esports news, given what you know about teams, players,
tournaments and orgs worldwide?" — which is exactly the kind of world-
knowledge lookup a keyword list structurally cannot do.

COST / QUOTA DESIGN
--------------------
Google's Gemini free tier (Flash-Lite family, as of mid-2026) is roughly
~15 requests/minute and ~1,000 requests/day per project. To stay WAY
under that with zero risk of ever paying anything or breaking the run:
  - DAILY_QUOTA_CAP: hard ceiling on AI calls per calendar day (UTC),
    tracked in state.json (state["gemini_quota"]) so it persists across
    the ~96 runs/day. Once hit, every remaining call this UTC day is
    skipped automatically (falls back to "keep the keyword verdict") —
    no crash, no error, just silently free again tomorrow.
  - PER_RUN_CAP: hard ceiling on AI calls within a SINGLE bot.py
    invocation, so one unusually newsy 15-minute window can't burn the
    whole day's budget by itself.
  - CALL_SPACING_SECONDS: a small sleep between consecutive AI calls in
    the same run, to stay under the per-minute limit even if PER_RUN_CAP
    is hit in one burst.

If GEMINI_API_KEY is not set at all, every function in this module is a
no-op that returns None immediately — the bot works exactly as it did
before this module existed, keyword-only, zero dependency.

Model choice: tries a short list of current Flash-Lite-class model IDs in
order and sticks with the first one that responds successfully for the
rest of the run (Google renames/deprecates model IDs every few months —
e.g. the whole Gemini 2.0 line was shut down June 2026 — so hardcoding a
single ID is fragile; this list gets updated occasionally as Google's
lineup changes, and any 404 just falls through to the next candidate).
"""

import os
import time
import logging
from datetime import datetime, timezone

import requests

log = logging.getLogger("ggnewsar-discord")

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()

# Tried in order; first one that returns a real response wins for the
# rest of this run. Update this list if Google renames/retires models —
# check https://ai.google.dev/gemini-api/docs/models for current IDs.
CANDIDATE_MODELS = [
    "gemini-flash-lite-latest",   # rolling alias, always points at current gen
    "gemini-2.5-flash-lite",
    "gemini-flash-latest",
]

API_BASE = "https://generativelanguage.googleapis.com/v1beta/models"
REQUEST_TIMEOUT_SECONDS = 8

# Quota safety margins — deliberately well under the documented free-tier
# ceiling (~1,000 RPD / ~15 RPM for Flash-Lite) so normal day-to-day
# variance never risks a paid charge or a hard block.
DAILY_QUOTA_CAP = 300
PER_RUN_CAP = 15
CALL_SPACING_SECONDS = 4.5

CLASSIFY_PROMPT_TEMPLATE = (
    "You are a strict classifier for an esports news bot. Answer with "
    "ONLY the single word YES or NO — no punctuation, no explanation.\n\n"
    "Question: is the following headline+summary genuine COMPETITIVE "
    "esports news? This includes: a specific esports team, player, "
    "coach, or org (roster moves, results, drama); a tournament, league, "
    "or match in a competitive video game (CS2, Valorant, League of "
    "Legends, Dota 2, PUBG, Mobile Legends, Overwatch, Rocket League, "
    "Rainbow Six, Call of Duty, EA FC/FIFAe, Fortnite competitive, "
    "fighting games, StarCraft, etc.) — even if the team/player/event "
    "name is one you don't specifically recognize, use context clues "
    "(prize pools, rosters, qualifiers, stand-ins, ESIC, franchise "
    "leagues, etc.) to judge; or esports business news (sponsorship, "
    "investment, acquisition, media rights) tied to an esports "
    "org/tournament.\n"
    "Answer NO for: single-player/story-mode games, hardware/shopping "
    "deals, traditional (non-esports) sports, general tech news, or "
    "anything not tied to competitive gaming.\n\n"
    "Headline: {title}\n"
    "Summary: {summary}"
)


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class GeminiBudget:
    """Tracks daily + per-run AI call counts. Load once per bot.py
    invocation from state["gemini_quota"], call .allow() before each
    classify() call, call .record() after a call actually goes out, and
    read back .to_state() at the end of the run to persist it."""

    def __init__(self, state_quota: dict | None):
        state_quota = state_quota or {}
        if state_quota.get("date") != _today_utc():
            # New UTC day (or first run ever) — reset the daily counter.
            state_quota = {"date": _today_utc(), "calls_today": 0}
        self.date = state_quota.get("date", _today_utc())
        self.calls_today = int(state_quota.get("calls_today", 0))
        self.calls_this_run = 0
        self._working_model = None  # sticky once one candidate succeeds

    def allow(self) -> bool:
        if not GEMINI_API_KEY:
            return False
        if self.calls_today >= DAILY_QUOTA_CAP:
            return False
        if self.calls_this_run >= PER_RUN_CAP:
            return False
        return True

    def record(self) -> None:
        self.calls_today += 1
        self.calls_this_run += 1

    def to_state(self) -> dict:
        return {"date": self.date, "calls_today": self.calls_today}


def _call_gemini(prompt: str, budget: GeminiBudget) -> str | None:
    """Low-level call. Tries candidate models in order (sticking with the
    first success). Returns the raw text response, or None on any
    failure (missing key, network error, 4xx/5xx, unexpected shape) —
    callers must treat None as 'no opinion', never as a NO."""
    models_to_try = [budget._working_model] if budget._working_model else CANDIDATE_MODELS
    for model in models_to_try:
        if not model:
            continue
        url = f"{API_BASE}/{model}:generateContent"
        try:
            resp = requests.post(
                url,
                headers={
                    "x-goog-api-key": GEMINI_API_KEY,
                    "Content-Type": "application/json",
                },
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"maxOutputTokens": 5, "temperature": 0},
                },
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            if resp.status_code == 404:
                # This model ID is gone/renamed — try the next candidate.
                continue
            if resp.status_code == 429:
                log.warning("Gemini 429 (rate limited) — stopping AI checks for this run")
                return None
            resp.raise_for_status()
            data = resp.json()
            candidates = data.get("candidates", [])
            if not candidates:
                continue
            parts = candidates[0].get("content", {}).get("parts", [])
            text = "".join(p.get("text", "") for p in parts).strip()
            if text:
                budget._working_model = model  # stick with what worked
                return text
        except requests.RequestException as e:
            log.warning(f"Gemini call failed ({model}): {e}")
            continue
    return None


def classify_relevance(title: str, summary: str, budget: GeminiBudget) -> bool | None:
    """Second-opinion check for an item the keyword filter did NOT
    accept. Returns True (AI thinks it's real esports news — send it),
    False (AI agrees it's not), or None (no AI available/quota
    exhausted/call failed — caller should fall back to the keyword
    filter's original verdict, i.e. reject, same as before this module
    existed)."""
    if not budget.allow():
        return None

    prompt = CLASSIFY_PROMPT_TEMPLATE.format(
        title=(title or "")[:300],
        summary=(summary or "")[:500],
    )
    budget.record()
    time.sleep(CALL_SPACING_SECONDS)  # stay comfortably under the RPM cap

    raw = _call_gemini(prompt, budget)
    if raw is None:
        return None
    answer = raw.strip().lower()
    if answer.startswith("yes"):
        return True
    if answer.startswith("no"):
        return False
    log.warning(f"Gemini returned unexpected text, ignoring: {raw[:50]!r}")
    return None


if __name__ == "__main__":
    # Quick manual smoke test (needs GEMINI_API_KEY in the environment).
    b = GeminiBudget(None)
    result = classify_relevance(
        "PlayTime Will Bring in Abed and Jabz on Games of the Future 2026",
        "After losing a couple players and their coach after the Esports "
        "World Cup 2026, PlayTime stood back up and started preparing "
        "for Games of the Future 2026.",
        b,
    )
    print("classify_relevance ->", result)
    print("budget state ->", b.to_state())
