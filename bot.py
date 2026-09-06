"""
GGNewsAR Discord Bot — RSS/scraper news pipeline (single-pass edition, hardened).

مشروع مستقل تماماً عن بوت تيليقرام. يجمع أخبار الرياضات الإلكترونية من كل
مصادر feeds.py (RSS + سكرابر للمصادر اللي ما عندها RSS)، يفلتر التكرار
والإعلانات وما يخص اللعبة نفسها بدل الايسبورتس، ويرسل لروم Discord عبر
Webhook. فيه طبقة جديدة (2026-09-06) لمنع تكرار نفس الخبر عبر أكثر من مصدر.

كل خبر RSS يمر أولاً على Gemini (مباشرة عبر Google AI Studio) اللي يحلله
ويطلع عنوان رئيسي وعنوان فرعي وملخص قصير بالفصحى البيضاء حسب ستايل
GGNewsAR، بدل إرسال عنوان/ملخص RSS الخام. لو التحليل فشل، يرجع البوت
تلقائياً للنص الأصلي.

=== ARCHITECTURE CHANGE (2026-07-05) ===
رجعنا لنمط single pass: كل استدعاء يفحص كل المصادر مرة وحدة ويطلع.
الاستمرارية (الفحص كل 10-15 دقيقة) تجيها من GitHub Actions schedule
(cron) في run.yml، مو من حلقة داخلية.

=== HARDENING PASS (2026-08-29) ===
سبب هذا التعديل: تقرير من حازم إن البوت وقف عن إرسال أي خبر لمدة ~9
ساعات رغم إن الـ cron كان يشتغل عادي كل 15 دقيقة، ورغم إن الـ webhook
مؤكد شغال. بمراجعة الكود القديم لقينا إنه:

1. تعليق run.yml كان يدّعي إنه فيه "checkpoint دوري (حفظ + commit) كل
   عدد قليل من الأخبار المُعالجة" — لكن هذا مو صحيح، الكود القديم كان
   يعمل save_state()+git_commit_push() *مرة وحدة بس بآخر الـ run*. يعني
   لو الـ run انقتل بمنتصف الطريق (تايم أوت 30 دقيقة، أو أي خطأ)، كل
   شغل الـ run هذا يضيع ولا يترّجل أي أثر بـ state.json، ولا حتى الأخبار
   يلي فعلاً انبعتت لـ Discord بنجاح.

2. انتظار Liquipedia لما يرجع "maxlag"/503 كان بيستخدم قيمة Retry-After
   يلي السيرفر يرجعها *بدون أي سقف*. لو رجعت قيمة كبيرة (أو تكررت
   الحالة أكثر من مرة بنفس الـ run)، الـ run يقعد نايم فعلياً لحد ما
   يوصل تايم أوت الـ 30 دقيقة الخارجي وينقتل.

3. أوامر git (commit/push/pull) ما كان إلها timeout إطلاقاً — لو تعلّق
   أمر git لأي سبب (شبكة، تعارض، إلخ)، الـ run يقعد عالق بصمت لحد
   تايم أوت الـ 30 دقيقة الخارجي، بدون أي رسالة تشرح ليش.

التعديلات بهذا الإصدار:
- SOFT_DEADLINE: ساعة داخلية (20 دقيقة) أقل من تايم أوت الـ run
  الخارجي (30 دقيقة) — أي حلقة أو مرحلة تتفقد هذا السقف وتوقف الشغل
  الجديد بأمان (مع حفظ ورفع الحالة) قبل ما GitHub يقتل الـ run بالقوة
  بدون أي تنظيف.
- Checkpoint دوري حقيقي: كل CHECKPOINT_EVERY_N_SENT رسالة تنبعث بنجاح
  بمرحلة RSS، نعمل save_state()+git_commit_push() فوراً، مو بس بآخر
  الـ run. هيك حتى لو انقتل الـ run بالنص، الأخبار يلي فعلاً انبعتت ما
  تتكرر بالـ run الجاي، ونعرف بالضبط وين وقفنا.
- سقف زمني على انتظار Liquipedia maxlag (LIQUIPEDIA_MAX_WAIT_SECONDS)
  + حد أقصى لعدد مرات الانتظار بنفس الـ run لكل wiki
  (LIQUIPEDIA_MAX_WAITS_PER_RUN) — بعدها نتخطى الـ wiki هذا بدل ما نضل
  ننتظر.
- timeout صريح على كل أوامر git.
- لوق أوضح بآخر كل run يوضح المدة الفعلية وسبب أي توقف مبكر.

=== RSS-ONLY + CROSS-SOURCE DEDUP (2026-09-06) ===
Two changes this pass, both driven by Hazem's report that the bot (a)
repeats some news and (b) isn't comprehensive enough / doesn't send
everything:

1. CROSS-SOURCE near-duplicate suppression. The old dedup only caught the
   SAME article twice (same URL / same normalized-title hash). It could not
   catch the SAME STORY across DIFFERENT outlets, so one roster move from
   HLTV + Dexerto + Dot Esports + Esports.gg went out four times. Each
   accepted story is now fingerprinted by its significant tokens and a new
   item that strongly overlaps one already sent within DEDUP_WINDOW_HOURS is
   dropped. See the dedup constants + story_fingerprint()/…_duplicate() below.

2. Comprehensiveness: per-run send caps raised (duplicates no longer eat the
   budget), and the scraper dispatch that feeds.py always assumed but bot.py
   never had is now wired in — so Sheep Esports (and any future scraped
   source) actually delivers instead of failing "no entries" every run.

The Liquipedia phase was removed entirely: watchlist.py no longer exists in
the repo (it was deleted 2026-08-22), so that phase had already been dead
code doing nothing every run — it now really is gone, along with its
maxlag-wait hang risk.

Pipeline (once per invocation):
1. Fetch every source in feeds.py IN PARALLEL (RSS/Atom, or a scraper for
   scraper-type sources).
2. Per item: freshness → URL/title dedup → spam/relevance/game-content
   filters → CROSS-SOURCE dedup → Gemini analysis → send. State is
   checkpointed periodically and at the end.

State is unified in state.json:
  - urls: seen RSS URLs (ring of last 8000)
  - title_hashes: normalized title hashes (ring of last 8000)
  - recent_fingerprints: story fingerprints sent within the dedup window
    (each {"t": tokens, "ts": epoch}), pruned to the window on load
  - liquipedia / last_liquipedia_check: legacy keys, tolerated on load from
    old state files but never written or read anymore.

Configuration source: feeds.py (RSS_FEEDS + optional SCRAPERS registry).
Secrets: DISCORD_WEBHOOK_URL, GEMINI_API_KEY in environment.

GitHub Actions workflow (run.yml) should trigger this via:
  on:
    workflow_dispatch:
    schedule:
      - cron: "*/15 * * * *"  # every 15 minutes
"""

import os
import re
import json
import time
import hashlib
import logging
import subprocess
import concurrent.futures
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

import feedparser
import requests

# Defensive sidecar imports (hardening, 2026-08-29): if feeds.py (or the
# relevance/scrapers modules below) were ever missing or broken in the
# checkout, a plain top-level import would crash before main() even runs —
# meaning ZERO news that pass. Each is imported defensively so a problem
# with one piece can never silence the whole bot.
try:
    from feeds import RSS_FEEDS
except ImportError as e:
    RSS_FEEDS = []
    _FEEDS_IMPORT_ERROR = str(e)
else:
    _FEEDS_IMPORT_ERROR = None

# SCRAPERS: optional registry in feeds.py for sources that expose NO working
# RSS/Atom endpoint and must be scraped instead (Sheep Esports' Next.js site
# is the first). Imported separately + defensively so a feeds.py without it
# (or an error inside a scraper module) can never crash the whole bot at
# import time. bot.py's fetch_one_feed() dispatches into this for any source
# marked {"fetch_type": "scraper", "scraper": "<key>"}.
#
# NOTE (2026-09-06): this dispatch was MISSING before today — feeds.py has
# defined the Sheep Esports scraper and this registry since 2026-08-22 and
# its own comments claimed "bot.py's fetch_one_feed() dispatches to it
# automatically", but bot.py never actually imported SCRAPERS or called it.
# So Sheep Esports silently failed every single run (a plain RSS fetch on a
# JS-rendered page returns 0 entries → "no entries" error). Wired in now.
try:
    from feeds import SCRAPERS
except (ImportError, AttributeError) as e:
    SCRAPERS = {}
    _SCRAPERS_IMPORT_ERROR = str(e)
else:
    _SCRAPERS_IMPORT_ERROR = None

# NEW (2026-08-30): keyword-based content filters — NO AI / NO API calls.
# relevance.py was written long ago but bot.py never actually imported or
# called it, so every item from every feed was sent with zero
# esports-relevance / spam / game-content filtering. Wired in now. Kept
# defensive like the two imports above: if the module is missing, the bot
# still runs (just without filtering) instead of crashing at import time.
try:
    from relevance import is_spam_ad, is_relevant_esports, is_game_content_noise, is_player_or_team_news
except ImportError as e:
    _REL_OK = False
    _RELEVANCE_IMPORT_ERROR = str(e)
else:
    _REL_OK = True
    _RELEVANCE_IMPORT_ERROR = None

# ============================================================
# Configuration
# ============================================================
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
STATE_FILE = Path("state.json")

# Cap to prevent flooding if many fresh items appear at once in one pass.
# NEW (2026-09-06): raised 50 → 150. With cross-source near-duplicate
# suppression now collapsing the same story reported by many outlets into
# one message (see the dedup layer below), the real message volume per pass
# drops a lot — so a higher ceiling no longer means a flood, it just means
# genuinely distinct stories are never left undelivered / aged out of the
# freshness window because the pass ran out of budget. Directly serves
# "أريده شامل / لا يرسل كل شي".
MAX_MESSAGES_PER_RUN = 150

# NEW (2026-08-30): per-source cap within a single run. A single very
# high-volume feed (e.g. Inven Global's LCK/LPL coverage) used to be able
# to eat the entire MAX_MESSAGES_PER_RUN budget on its own, starving every
# other source. Now no single source sends more than this many items per
# run; the rest are left for the next run (not lost). Paces flooders and
# guarantees breadth across sources.
# NEW (2026-09-06): raised 8 → 20 alongside the global cap, for the same
# comprehensiveness reason — a legitimately busy source (a Major weekend on
# HLTV/VLR) shouldn't be throttled to 8 stories per pass now that the global
# ceiling is much higher and duplicates are collapsed.
MAX_PER_SOURCE_PER_RUN = 20

# Discord webhook rate limit safety margin
MESSAGE_DELAY_SECONDS = 1.0

# RSS freshness window: ignore items older than this
MAX_AGE_HOURS = 24

# State ring sizes
SEEN_URLS_RING = 8000
SEEN_TITLES_RING = 8000

# RSS parallel fetch settings
RSS_FETCH_WORKERS = 40
RSS_FETCH_TIMEOUT_SECONDS = 10

# ------------------------------------------------------------
# Cross-source near-duplicate suppression (NEW 2026-09-06)
# ------------------------------------------------------------
# THE FIX FOR "يكرر بعض الأخبار". The old dedup only caught the SAME item
# twice: same URL, or same normalized-title hash. It could not catch the
# SAME STORY reported by DIFFERENT outlets — one roster move covered by
# HLTV, Dexerto, Dot Esports and Esports.gg produced four different URLs
# and four different titles, so all four were sent. This layer fingerprints
# each story by its significant tokens (title + lead of its summary) and
# suppresses any new item that strongly overlaps a story already sent within
# the window below. It runs AFTER the spam/relevance filters, so only real
# esports stories seed the fingerprint set.
DEDUP_WINDOW_HOURS = 18
DEDUP_FINGERPRINTS_RING = 2000
# A candidate is judged a duplicate of an already-sent story ONLY when the
# two share at least DEDUP_MIN_SHARED significant tokens AND the overlap
# coefficient (shared / smaller token set) is >= DEDUP_OVERLAP.
#
# TUNING (2026-09-06, option أ — comprehensive-first, per Hazem): these are
# set DELIBERATELY STRICT so the bot never merges two genuinely different
# stories. Calibration showed the unavoidable hard case is two DIFFERENT
# matches of the SAME team on a tournament day ("Falcons beat Vitality 2-0"
# vs "Falcons beat NAVI 2-1") — those share a long template and land around
# overlap 0.60. Requiring overlap >= 0.75 keeps BOTH of those (no lost
# result), while still collapsing the dominant real-duplicate pattern: the
# same story re-reported near-verbatim by many general outlets (Dexerto,
# Dot Esports, ESTNN, Esports.gg all echoing an HLTV/VLR break), which lands
# ~0.79+. The accepted tradeoff of option أ: a heavily-reworded retelling of
# one story may occasionally slip through as a second post — chosen on
# purpose over ever dropping a real, distinct story.
#
# There is intentionally NO raw-shared-count shortcut: a pure "share N
# tokens → duplicate" rule was what risked merging those long-template
# distinct matches (they can share 9+ tokens), so it was removed.
DEDUP_MIN_SHARED = 4
DEDUP_OVERLAP = 0.75

# Discord embed color
EMBED_COLOR = 0x7C3AED
DESC_MAX = 600

# Strip "Source - Article Title" patterns from RSS titles for dedup
SOURCE_SUFFIX_RE = re.compile(r"\s*[\-\|\u2013\u2014:]\s*[^\-\|\u2013\u2014:]{1,40}$")

# NEW (hardening): internal soft deadline, well under the job's external
# timeout-minutes (30) in run.yml. Any phase/loop checks this and bails
# out cleanly — saving + pushing whatever progress was made — instead of
# letting GitHub hard-kill the process with zero cleanup.
SOFT_DEADLINE_SECONDS = 20 * 60  # 20 minutes
RUN_STARTED_AT = time.monotonic()

# NEW (hardening): persist state to disk + git every N successful Discord
# sends during the RSS phase, not just once at the very end of the run.
CHECKPOINT_EVERY_N_SENT = 5

# NEW (hardening): explicit timeout for every git subprocess call, so a
# stalled push/pull can't hang the run silently.
GIT_SUBPROCESS_TIMEOUT = 60


def time_left() -> float:
    """Seconds remaining before the internal soft deadline."""
    return SOFT_DEADLINE_SECONDS - (time.monotonic() - RUN_STARTED_AT)


def deadline_exceeded() -> bool:
    return time_left() <= 0


# ------------------------------------------------------------
# Gemini (direct via Google AI Studio) — news analysis
# ------------------------------------------------------------
GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
GEMINI_TIMEOUT_SECONDS = 20
GEMINI_MAX_RETRIES = 2
GEMINI_MAX_TOKENS = 800

GEMINI_SYSTEM_PROMPT = """أنت محرر أخبار إسبورت لمنصة GGNewsAR، تكتب بالعربية الفصحى البيضاء (لغة يومية مثقفة، مو لغة أدبية أو مترجمة حرفياً).

مهمتك: تحليل خبر إسبورت وإخراج ثلاثة عناصر: عنوان رئيسي، عنوان فرعي، وملخص قصير.

قواعد صارمة:
- العنوان الرئيسي: لازم يحتوي اسم اللعبة، يبدأ بأهم معلومة (رقم/إنجاز/حدث)، وينتهي بعلامة استفهام أو تعجب حسب نوع الخبر. لو الخبر عن شراكة أو اتفاق أو تحالف، افتح بكلمة صادمة زي "شراكة!" أو "اتفاق رسمي!" أو "تحالف ضخم!".
- العنوان الفرعي: جملة واحدة قصيرة تضيف تفصيل أو سياق إضافي لم يُذكر بالعنوان الرئيسي، مش تكرار له.
- الملخص: جملتين أو ثلاث قصيرة ومتتالية، تبدأ بفعل مباشر (تأهل، حسم، أنهى، خطف)، أرقام وأسماء بالمقدمة، بدون نقاط أو عناوين فرعية.
- ممنوع أي عبارات حشو أو توحي بالذكاء الاصطناعي مثل: "يأتي ذلك في إطار"، "في خطوة لافتة"، "يُعد علامة فارقة"، "تجدر الإشارة إلى"، "من الجدير بالذكر"، "وفي سياق متصل"، "يُشكل نقلة نوعية"، وصفات فارغة مثل "كبيرة" أو "بارزة" بدون وزن فعلي.
- أسماء اللاعبين: اللقب فقط (Nickname)، بدون الاسم الحقيقي الكامل.
- الأرقام المالية: أرقام كاملة مع فواصل الآلاف (مثال: 1,000,000)، ما تكتبها بالحروف.
- لو فيه أكثر من فريق عربي بنفس الخبر، لا تبرز فريق واحد بالعنوان دون مبرر واضح من الخبر نفسه.
- لو المصدر ما فيه معلومات كافية لتأكيد تفصيل معين، لا تختلقه.

رد بصيغة JSON فقط، بدون أي نص أو شرح إضافي قبله أو بعده، بالشكل التالي بالضبط:
{"headline": "...", "subheadline": "...", "summary": "..."}"""


def analyze_with_gemini(title: str, summary: str, link: str) -> dict | None:
    """
    Analyze one news item via Gemini (Google AI Studio free tier, direct API).
    Returns {"headline": ..., "subheadline": ..., "summary": ...} or None on failure.
    """
    if not GEMINI_API_KEY:
        return None

    user_content = (
        f"العنوان الأصلي: {title}\n\n"
        f"محتوى/ملخص الخبر: {summary or 'غير متوفر'}\n\n"
        f"رابط المصدر: {link}"
    )

    payload = {
        "system_instruction": {"parts": [{"text": GEMINI_SYSTEM_PROMPT}]},
        "contents": [{"role": "user", "parts": [{"text": user_content}]}],
        "generationConfig": {
            "temperature": 0.4,
            "maxOutputTokens": GEMINI_MAX_TOKENS,
            "responseMimeType": "application/json",
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }
    headers = {
        "x-goog-api-key": GEMINI_API_KEY,
        "Content-Type": "application/json",
    }

    for attempt in range(GEMINI_MAX_RETRIES):
        try:
            r = requests.post(
                GEMINI_URL, json=payload, headers=headers, timeout=GEMINI_TIMEOUT_SECONDS
            )
            if r.status_code == 429:
                time.sleep(2)
                continue
            r.raise_for_status()
            data = r.json()
            candidates = data.get("candidates", [])
            if not candidates:
                log.warning(f"Gemini returned no candidates (attempt {attempt + 1}/{GEMINI_MAX_RETRIES}): {data}")
                time.sleep(1)
                continue
            parts = candidates[0].get("content", {}).get("parts", [])
            content = parts[0].get("text") if parts else None
            if not content or not str(content).strip():
                log.warning(f"Gemini returned empty/None content (attempt {attempt + 1}/{GEMINI_MAX_RETRIES})")
                time.sleep(1)
                continue
            content = content.strip()
            content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.MULTILINE).strip()
            parsed = json.loads(content)
            if all(k in parsed and parsed[k] for k in ("headline", "subheadline", "summary")):
                return parsed
            log.warning(f"Gemini response missing/empty keys: {content[:200]}")
            return None
        except (requests.RequestException, ValueError, KeyError, AttributeError, json.JSONDecodeError) as e:
            log.warning(f"Gemini analysis failed (attempt {attempt + 1}/{GEMINI_MAX_RETRIES}): {e}")
            time.sleep(1)
    return None


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ggnewsar-discord")


# ============================================================
# State persistence
# ============================================================
def _empty_state() -> dict:
    """The canonical empty state. Every load path returns a dict with all
    expected keys present, so downstream code never has to guard for a
    missing collection."""
    return {
        "urls": [],
        "title_hashes": [],
        "recent_fingerprints": [],  # [{"t": tokens, "ts": epoch}, ...]
        # Legacy keys — tolerated for old state.json files, never used now.
        "liquipedia": {},
        "last_liquipedia_check": None,
    }


def load_state() -> dict:
    if not STATE_FILE.exists():
        return _empty_state()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.error(f"state.json corrupted, starting fresh: {e}")
        return _empty_state()
    for k, v in _empty_state().items():
        data.setdefault(k, v)
    # Prune cross-source fingerprints older than the dedup window so the
    # ring stays small and a story can be re-reported after the window.
    cutoff = time.time() - DEDUP_WINDOW_HOURS * 3600
    data["recent_fingerprints"] = [
        r for r in data["recent_fingerprints"]
        if isinstance(r, dict) and r.get("ts", 0) >= cutoff
    ]
    return data


def save_state(state: dict) -> None:
    state["urls"] = state["urls"][-SEEN_URLS_RING:]
    state["title_hashes"] = state["title_hashes"][-SEEN_TITLES_RING:]
    state["recent_fingerprints"] = state.get("recent_fingerprints", [])[-DEDUP_FINGERPRINTS_RING:]
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def git_commit_push(reason: str = "") -> None:
    """Commit + push state.json if it changed. Safe to call even when
    nothing changed — no-ops cleanly. Never raises: a failed push here
    should not crash the run, just gets retried on the next invocation.

    NEW (hardening): every git call now has an explicit timeout, so a
    stalled pull/push can't hang the whole run silently.
    """
    try:
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"],
                        check=True, capture_output=True, timeout=GIT_SUBPROCESS_TIMEOUT)
        subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"],
                        check=True, capture_output=True, timeout=GIT_SUBPROCESS_TIMEOUT)
        subprocess.run(["git", "add", "state.json"], check=True, capture_output=True,
                        timeout=GIT_SUBPROCESS_TIMEOUT)
        diff = subprocess.run(["git", "diff", "--cached", "--quiet"], timeout=GIT_SUBPROCESS_TIMEOUT)
        if diff.returncode == 0:
            return  # nothing changed, nothing to commit
        msg = "chore: update state.json [skip ci]"
        if reason:
            msg += f" ({reason})"
        subprocess.run(["git", "commit", "-m", msg], check=True, capture_output=True,
                        timeout=GIT_SUBPROCESS_TIMEOUT)
        subprocess.run(["git", "pull", "--rebase", "origin", "main"], capture_output=True,
                        timeout=GIT_SUBPROCESS_TIMEOUT)
        r = subprocess.run(["git", "push"], capture_output=True, text=True, timeout=GIT_SUBPROCESS_TIMEOUT)
        if r.returncode != 0:
            log.warning(f"git push failed: {r.stderr[:300]}")
    except subprocess.TimeoutExpired as e:
        log.warning(f"git command timed out after {GIT_SUBPROCESS_TIMEOUT}s (reason={reason}): {e}")
    except subprocess.CalledProcessError as e:
        log.warning(f"git commit/push step failed (reason={reason}): {e}")


def checkpoint(state: dict, reason: str) -> None:
    """Save + commit + push state right now. Used both at the end of a
    normal pass and periodically/on-early-exit so progress is never lost
    to a killed or timed-out run."""
    save_state(state)
    git_commit_push(reason)


# ============================================================
# Discord
# ============================================================
def _clip(text: str, limit: int) -> str:
    if not text:
        return ""
    text = text.strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def send_discord(title: str, link: str = "", source: str = "", summary: str = "", image_url: str = "", is_player_team: bool = False) -> bool:
    """Send one news item to Discord as an embed. Returns True on success."""
    if not DISCORD_WEBHOOK_URL:
        log.error("Discord webhook missing")
        return False

    embed = {
        "title": _clip(title, 256),
        "color": EMBED_COLOR,
    }
    if link:
        embed["url"] = link
    if summary:
        embed["description"] = _clip(summary, DESC_MAX)
    if source:
        embed["footer"] = {"text": _clip(source, 2048)}
    if image_url:
        embed["image"] = {"url": image_url}
    # NEW (2026-08-30): visible "🎯 لاعب/فريق" badge for player/team-specific
    # items (statement, interview, transfer, life event), so they stand out
    # in the same mixed channel instead of blending into tournament/business
    # news. Pure display — never affects whether the item was sent.
    if is_player_team:
        embed["fields"] = [{"name": "النوع", "value": "🎯 لاعب/فريق", "inline": True}]

    payload = {"embeds": [embed]}

    for attempt in range(3):
        try:
            r = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=15)
            if r.status_code in (200, 204):
                return True
            if r.status_code == 429:
                retry_after = r.json().get("retry_after", 1)
                time.sleep(float(retry_after) + 0.5)
                continue
            log.error(f"Discord {r.status_code}: {r.text[:200]}")
            return False
        except requests.RequestException as e:
            log.error(f"Discord request failed (attempt {attempt + 1}): {e}")
            time.sleep(2)
    return False


# ============================================================
# RSS phase
# ============================================================
# NEW (2026-08-30): dedup helpers. The old normalize_title only stripped a
# single trailing "- Source" segment, lowercased, and dropped punctuation,
# then hashed exactly. So the SAME story arriving with tiny wording/format
# differences (Google-News "Title - Publisher" variants, Arabic diacritics,
# an extra stopword, a different quote style) produced a DIFFERENT hash and
# was re-sent — the repeated-Inven-Global symptom. This version also:
#   - strips the "- Publisher" suffix up to twice (Google News sometimes
#     appends "- A - B"),
#   - removes Arabic diacritics + tatweel,
#   - unifies Arabic letter variants (أإآ→ا, ى→ي, ة→ه, ؤئ),
#   - drops short/stopwords.
# It deliberately KEEPS word order (does NOT sort tokens), so genuinely
# different stories that share words — especially reversed match results
# like "Falcons beat Vitality" vs "Vitality beat Falcons" — still hash
# differently and are both delivered.
_AR_DIACRITICS_RE = re.compile(r"[\u0617-\u061A\u064B-\u0652\u0670\u0640]")
_TITLE_STOPWORDS = {
    # English
    "the", "a", "an", "of", "to", "in", "on", "at", "for", "and", "or",
    "with", "as", "is", "are", "was", "were", "by", "from", "vs", "vs.",
    # Arabic
    "في", "من", "على", "الى", "إلى", "عن", "مع", "و", "او", "أو", "ال",
}


def normalize_title(title: str) -> str:
    t = title.lower().strip()
    # Strip a trailing "- Publisher"-style suffix up to twice.
    for _ in range(2):
        new_t = SOURCE_SUFFIX_RE.sub("", t).strip()
        if new_t == t:
            break
        t = new_t
    t = _AR_DIACRITICS_RE.sub("", t)
    for a, b in (("أ", "ا"), ("إ", "ا"), ("آ", "ا"), ("ى", "ي"),
                 ("ة", "ه"), ("ؤ", "و"), ("ئ", "ي")):
        t = t.replace(a, b)
    t = re.sub(r"[^\w\s]", " ", t)
    # Drop stopwords, but KEEP single-char/digit tokens: they carry meaning
    # in esports titles (team tags like "G2"/"C9", scorelines like "2-0" vs
    # "0-2", roster slots) and are exactly what tells reversed results apart.
    tokens = [w for w in t.split() if w not in _TITLE_STOPWORDS]
    return " ".join(tokens)


def title_hash(title: str) -> str:
    return hashlib.md5(normalize_title(title).encode("utf-8")).hexdigest()


# ------------------------------------------------------------
# Cross-source near-duplicate fingerprinting (NEW 2026-09-06)
# ------------------------------------------------------------
_DEDUP_TOKEN_MIN_LEN = 3


def story_fingerprint(title: str, summary: str = "") -> set:
    """Significant-token set identifying a STORY (not a specific article),
    for catching the same event across different outlets. Reuses
    normalize_title's cleaning (lowercase, publisher-suffix strip, Arabic
    normalization, stopword + punctuation removal) on the title plus the
    lead of the summary, then keeps tokens that are >= _DEDUP_TOKEN_MIN_LEN
    chars OR contain a digit — scorelines, years and prize numbers are
    strong, reword-resistant anchors that the same story keeps across
    outlets."""
    combined = f"{title} {strip_html(summary)[:200]}"
    toks = normalize_title(combined).split()
    return {w for w in toks if len(w) >= _DEDUP_TOKEN_MIN_LEN or any(c.isdigit() for c in w)}


def is_cross_source_duplicate(fingerprint: set, recent_sets: list) -> bool:
    """True if this story's fingerprint strongly overlaps a story already
    sent within the dedup window. recent_sets is the pre-built list of
    token sets for state['recent_fingerprints'] (already pruned to the
    window). Comprehensive-first (option أ): a single strict rule —
    share >= DEDUP_MIN_SHARED significant tokens AND overlap coefficient
    >= DEDUP_OVERLAP — so only near-identical retellings merge and two
    distinct stories are never collapsed. See the dedup constants above."""
    if not fingerprint:
        return False
    fp_len = len(fingerprint)
    for prev in recent_sets:
        if not prev:
            continue
        shared = len(fingerprint & prev)
        if shared >= DEDUP_MIN_SHARED and shared / min(fp_len, len(prev)) >= DEDUP_OVERLAP:
            return True
    return False


def record_fingerprint(fingerprint: set, state: dict, recent_sets: list) -> None:
    """Register a story as 'already covered this window' — into both the
    persisted ring (state) and the in-run comparison list (recent_sets),
    so later items in the SAME pass are deduped against it too."""
    if not fingerprint:
        return
    fps = state.setdefault("recent_fingerprints", [])
    fps.append({"t": " ".join(sorted(fingerprint)), "ts": time.time()})
    if len(fps) > DEDUP_FINGERPRINTS_RING:
        del fps[:-DEDUP_FINGERPRINTS_RING]
    recent_sets.append(fingerprint)


def is_fresh(entry, max_age_hours: int) -> bool:
    """True if entry has no timestamp or is within freshness window."""
    pub = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if not pub:
        return True
    try:
        pub_time = datetime(*pub[:6], tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return True
    return (datetime.now(timezone.utc) - pub_time) <= timedelta(hours=max_age_hours)


def strip_html(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


IMG_TAG_RE = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.IGNORECASE)


def extract_image(entry) -> str:
    """Best-effort image extraction from an RSS/Atom entry. Returns '' if none found."""
    media_content = entry.get("media_content")
    if media_content:
        for m in media_content:
            url = m.get("url")
            if url:
                return url

    media_thumb = entry.get("media_thumbnail")
    if media_thumb:
        for m in media_thumb:
            url = m.get("url")
            if url:
                return url

    for link_obj in entry.get("links", []):
        if str(link_obj.get("type", "")).startswith("image/"):
            href = link_obj.get("href")
            if href:
                return href

    raw_html = entry.get("summary") or entry.get("description") or ""
    content_list = entry.get("content")
    if content_list:
        raw_html = content_list[0].get("value", raw_html)

    match = IMG_TAG_RE.search(raw_html)
    if match:
        return match.group(1)
    return ""


def fetch_one_feed(feed_info: dict):
    """Fetch + parse a single feed. Never raises — returns (name, entries_or_None, error_or_None).

    Called from a thread pool so all sources are fetched concurrently
    instead of one-by-one, keeping each single-pass invocation fast
    (seconds, not minutes) even with a slow/dead source mixed in."""
    name = feed_info["name"]

    # NEW (2026-09-06): scraper dispatch. A source with no working RSS/Atom
    # endpoint sets {"fetch_type": "scraper", "scraper": "<key>"} and is
    # handled by feeds.SCRAPERS[<key>] instead of a normal fetch. The scraper
    # returns the exact same (name, entries, error) 3-tuple, so the rest of
    # the pipeline needs no special-casing. This is what finally makes Sheep
    # Esports (and any future scraped source) actually deliver instead of
    # failing "no entries" every run.
    if feed_info.get("fetch_type") == "scraper":
        key = feed_info.get("scraper", "")
        scraper_fn = SCRAPERS.get(key)
        if not scraper_fn:
            return name, None, f"scraper '{key}' not in SCRAPERS registry"
        try:
            return scraper_fn(feed_info)
        except Exception as e:  # scrapers promise never to raise; be safe anyway
            return name, None, f"scraper '{key}' raised: {e}"

    url = feed_info["url"]
    try:
        resp = requests.get(
            url,
            timeout=RSS_FETCH_TIMEOUT_SECONDS,
            headers={"User-Agent": "Mozilla/5.0 (compatible; GGNewsARBot/1.0)"},
        )
        resp.raise_for_status()
        d = feedparser.parse(resp.content)
        if d.bozo and not d.entries:
            raise RuntimeError(f"bozo={d.bozo_exception or d.bozo}")
        if not d.entries:
            raise RuntimeError("no entries")
        return name, d.entries, None
    except Exception as e:
        return name, None, str(e)


def rss_phase(state: dict, first_run: bool, sent_budget: int) -> int:
    """Run RSS collection. Fetches all sources in parallel, then processes
    (dedup + Gemini + send) sequentially so Discord rate limiting and the
    send budget stay predictable. Returns number of messages sent.

    NEW (hardening): checks the soft deadline before every item and bails
    out cleanly (with a checkpoint) if it's exceeded, and checkpoints state
    every CHECKPOINT_EVERY_N_SENT successful sends instead of only once at
    the very end of the whole run."""
    seen_urls = set(state["urls"])
    seen_titles = set(state["title_hashes"])
    # NEW (2026-09-06): in-run comparison list for cross-source dedup. Built
    # once from the persisted (already window-pruned) fingerprints, then
    # appended to as we send during this pass — so two outlets covering the
    # same story WITHIN one pass also collapse to a single message.
    recent_sets = [set(r["t"].split()) for r in state.get("recent_fingerprints", []) if r.get("t")]
    stats = defaultdict(int)
    failed = []
    sent = 0
    since_last_checkpoint = 0

    # NEW (2026-08-30): look up each source's metadata (priority, loose_query)
    # by name so we can (a) process high-priority sources first and (b) apply
    # the loose_query relevance gate. Names are unique (feeds.py checks dupes).
    feed_meta = {fi["name"]: fi for fi in RSS_FEEDS}
    _PRIO_RANK = {"high": 0, "normal": 1, "low": 2}
    sent_per_source = defaultdict(int)

    log.info(f"RSS phase: {len(RSS_FEEDS)} sources, {RSS_FETCH_WORKERS} parallel workers, freshness={MAX_AGE_HOURS}h")

    fetch_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=RSS_FETCH_WORKERS) as executor:
        futures = {executor.submit(fetch_one_feed, fi): fi for fi in RSS_FEEDS}
        for future in concurrent.futures.as_completed(futures):
            fetch_results.append(future.result())

    # NEW (2026-08-30): process sources in PRIORITY order (high → normal →
    # low), not in the random order they happened to finish downloading in.
    # feeds.py has documented a "priority" field since 2026-07-05 and even
    # claimed it was "genuinely applied" on 2026-08-11 — but bot.py never
    # actually sorted by it, so it was a no-op. Now it's real: if the send
    # budget is ever exhausted, primary/official sources (JEF, EWC, HLTV/
    # VLR-tier, official team blogs) are the ones that got through, not
    # whichever feed's HTTP response came back first.
    fetch_results.sort(
        key=lambda r: _PRIO_RANK.get(feed_meta.get(r[0], {}).get("priority", "normal"), 1)
    )

    for name, entries, error in fetch_results:
        if error:
            stats["sources_failed"] += 1
            failed.append(f"{name}: {error}")
            continue
        stats["sources_ok"] += 1

        for entry in entries:
            if deadline_exceeded():
                stats["aborted_deadline"] += 1
                log.warning(
                    "Soft deadline reached during RSS phase — stopping early and "
                    "checkpointing so nothing already sent gets lost or repeated."
                )
                checkpoint(state, "deadline hit mid-RSS-phase")
                _log_rss_summary(stats, failed)
                return sent

            stats["entries_total"] += 1
            link = (entry.get("link") or "").strip()
            title = (entry.get("title") or "").strip()
            summary = entry.get("summary") or entry.get("description") or ""

            if not link or not title:
                stats["skip_no_link_or_title"] += 1
                continue

            if link in seen_urls:
                stats["skip_seen_url"] += 1
                continue

            t_hash = title_hash(title)

            if not is_fresh(entry, MAX_AGE_HOURS):
                stats["skip_old"] += 1
                seen_urls.add(link); state["urls"].append(link)
                seen_titles.add(t_hash); state["title_hashes"].append(t_hash)
                continue

            if t_hash in seen_titles:
                stats["skip_dup_title"] += 1
                seen_urls.add(link); state["urls"].append(link)
                continue

            # NEW (2026-08-30): keyword content filters (no AI). Applied to
            # FRESH, non-duplicate items only. Dropped items are marked seen
            # so we don't re-evaluate them every run until they age out.
            if _REL_OK:
                fi = feed_meta.get(name, {})
                filter_text = f"{title} {strip_html(summary)}"

                # 1) Spam / ad / channel-recruitment — checked on EVERY
                #    source unconditionally (a compromised/ad-injected feed
                #    can be any source). This is the permanent fix for the
                #    recurring "join our Telegram — A-TOOLS X" ad incidents.
                if is_spam_ad(filter_text, link):
                    stats["skip_spam_ad"] += 1
                    seen_urls.add(link); state["urls"].append(link)
                    seen_titles.add(t_hash); state["title_hashes"].append(t_hash)
                    continue

                # 2) Esports relevance — only for broad Google-News keyword
                #    bridges (loose_query). Dedicated esports RSS feeds and
                #    official accounts are trusted and skip this gate, per
                #    relevance.py's own usage note. This is what stops a
                #    country-name search from surfacing a wildfire/festival
                #    article that merely mentions the country + "esports".
                #    NEW (2026-08-30): also keep it if it reads as
                #    player/team-specific news (statement, transfer, life
                #    event) even without a game-name/entity match — a real
                #    gap for interview-style headlines like "PlayerName on
                #    fixing mistakes..." that don't repeat the org/game name.
                if fi.get("loose_query") and not (
                    is_relevant_esports(filter_text) or is_player_or_team_news(filter_text)
                ):
                    stats["skip_not_esports"] += 1
                    seen_urls.add(link); state["urls"].append(link)
                    seen_titles.add(t_hash); state["title_hashes"].append(t_hash)
                    continue

                # 3) Pure game content (patch notes, skins, guides, reviews,
                #    cosplay) with NO tie to the pro/competitive scene, an
                #    org, a tournament, or a person's org move. GGNewsAR is an
                #    esports wire, not a games wire — this is "بعضها لا يهمني
                #    في الايسبورتس". Conservative by design: anything carrying
                #    a competitive/business/roster signal is kept.
                if is_game_content_noise(filter_text):
                    stats["skip_game_content"] += 1
                    seen_urls.add(link); state["urls"].append(link)
                    seen_titles.add(t_hash); state["title_hashes"].append(t_hash)
                    continue

            # NEW (2026-09-06): CROSS-SOURCE near-duplicate check — the fix
            # for "يكرر بعض الأخبار". Even though this exact URL/title hasn't
            # been seen, the same STORY may already have been sent from
            # another outlet within the window. Fingerprint by significant
            # tokens and drop if it strongly overlaps one already covered.
            # Marked seen (so we don't reprocess it) but NOT sent again.
            fingerprint = story_fingerprint(title, summary)
            if not first_run and is_cross_source_duplicate(fingerprint, recent_sets):
                stats["skip_cross_dup"] += 1
                seen_urls.add(link); state["urls"].append(link)
                seen_titles.add(t_hash); state["title_hashes"].append(t_hash)
                continue

            # Passes all gates. Mark seen regardless of send outcome.
            seen_urls.add(link); state["urls"].append(link)
            seen_titles.add(t_hash); state["title_hashes"].append(t_hash)

            if first_run:
                # Seed the fingerprint set during baseline indexing too, so
                # the first real pass doesn't re-blast a story that was
                # already present (from any outlet) at baseline time.
                record_fingerprint(fingerprint, state, recent_sets)
                stats["baseline_recorded"] += 1
                continue

            if sent >= sent_budget:
                stats["skip_cap"] += 1
                state["urls"].pop()
                state["title_hashes"].pop()
                seen_urls.discard(link)
                seen_titles.discard(t_hash)
                continue

            # NEW (2026-08-30): per-source cap. Undo the seen-marks (exactly
            # like the global cap above) so these items are NOT lost — they
            # stay eligible and flow through on the next run(s), just paced so
            # one high-volume feed can't dominate a single pass.
            if sent_per_source[name] >= MAX_PER_SOURCE_PER_RUN:
                stats["skip_source_cap"] += 1
                state["urls"].pop()
                state["title_hashes"].pop()
                seen_urls.discard(link)
                seen_titles.discard(t_hash)
                continue

            clean_summary = strip_html(summary)
            # NEW (2026-08-30): tag player/team-specific items (statement,
            # interview, transfer, life event) so they're visibly
            # distinguishable in the same mixed channel. Pure tagging —
            # never affects whether the item was already accepted above.
            is_player_team = _REL_OK and is_player_or_team_news(filter_text)
            analysis = analyze_with_gemini(title, clean_summary, link)
            if analysis:
                stats["gemini_analyzed"] += 1
                send_title = analysis["headline"]
                send_desc = f"**{analysis['subheadline']}**\n\n{analysis['summary']}"
            else:
                stats["gemini_fallback"] += 1
                send_title = title
                send_desc = clean_summary[:280]

            ok = send_discord(
                title=send_title,
                link=link,
                source=name,
                summary=send_desc,
                image_url=extract_image(entry),
                is_player_team=is_player_team,
            )
            if ok:
                sent += 1
                sent_per_source[name] += 1
                stats["sent"] += 1
                since_last_checkpoint += 1
                # Register this story so any later item (this pass or a
                # future one, within the window) reporting the same event is
                # suppressed as a cross-source duplicate.
                record_fingerprint(fingerprint, state, recent_sets)
                time.sleep(MESSAGE_DELAY_SECONDS)
            else:
                stats["send_failures"] += 1

            if since_last_checkpoint >= CHECKPOINT_EVERY_N_SENT:
                checkpoint(state, "periodic checkpoint")
                since_last_checkpoint = 0

    _log_rss_summary(stats, failed)
    return sent


def _log_rss_summary(stats: dict, failed: list) -> None:
    log.info("--- RSS Summary ---")
    for k in sorted(stats.keys()):
        log.info(f" {k:30s} {stats[k]}")
    if failed:
        log.info(f"--- Failed Sources ({len(failed)}) ---")
        for line in failed:
            log.info(f" - {line}")


# ============================================================
# Main — single pass
# ============================================================
def main():
    if not DISCORD_WEBHOOK_URL:
        log.error("Missing DISCORD_WEBHOOK_URL env var")
        return
    if not GEMINI_API_KEY:
        log.warning("Missing GEMINI_API_KEY — Gemini analysis disabled, will fall back to raw RSS titles/summaries.")

    # NEW (hardening): surface sidecar-import problems loudly and clearly,
    # right at the top of the log, instead of a bare crash with no context.
    if _FEEDS_IMPORT_ERROR:
        log.error(
            f"feeds.py could not be imported ({_FEEDS_IMPORT_ERROR}) — "
            f"RSS phase will have 0 sources this pass. Check that feeds.py "
            f"exists at the repo root next to bot.py."
        )
    if _SCRAPERS_IMPORT_ERROR:
        log.warning(
            f"SCRAPERS registry could not be imported from feeds.py "
            f"({_SCRAPERS_IMPORT_ERROR}) — any scraper-based source "
            f"(e.g. Sheep Esports) will fail this pass; normal RSS sources "
            f"are unaffected."
        )
    if not _REL_OK:
        log.warning(
            f"relevance.py could not be imported ({_RELEVANCE_IMPORT_ERROR}) — "
            f"content filtering (spam/esports-relevance/game-content) is "
            f"DISABLED this pass; every fresh item will be sent unfiltered. "
            f"Check that relevance.py exists at the repo root next to bot.py."
        )

    log.info(
        f"=== Pass starting. soft_deadline={SOFT_DEADLINE_SECONDS}s, "
        f"checkpoint_every={CHECKPOINT_EVERY_N_SENT} sends ==="
    )

    state = load_state()
    first_run = (
        len(state["urls"]) == 0
        and len(state["title_hashes"]) == 0
    )
    if first_run:
        log.info("FIRST RUN: indexing baseline, no messages will be sent this pass.")

    rss_sent = rss_phase(state, first_run, MAX_MESSAGES_PER_RUN)

    checkpoint(state, "end of pass")

    elapsed = time.monotonic() - RUN_STARTED_AT
    log.info(f"=== Pass done in {elapsed:.1f}s. RSS sent: {rss_sent} ===")


if __name__ == "__main__":
    main()
