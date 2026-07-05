"""
GGNewsAR Discord Bot — unified RSS + Liquipedia pipeline (single-pass edition).

مشروع مستقل تماماً عن بوت تيليقرام. نفس المنطق بالضبط (RSS + Liquipedia +
dedup + state)، لكن الإرسال يروح لروم Discord عبر Webhook بدل تيليقرام.

كل خبر RSS يمر أولاً على Gemini (مباشرة عبر Google AI Studio) اللي يحلله
ويطلع عنوان رئيسي وعنوان فرعي وملخص قصير بالفصحى البيضاء حسب ستايل
GGNewsAR، بدل إرسال عنوان/ملخص RSS الخام. لو التحليل فشل، يرجع البوت
تلقائياً للنص الأصلي.

=== ARCHITECTURE CHANGE (2026-07-05) ===
رجعنا لنمط single pass: كل استدعاء يفحص كل المصادر مرة وحدة ويطلع.
الاستمرارية (الفحص كل 10-15 دقيقة) تجيها من GitHub Actions schedule
(cron) في run.yml، مو من حلقة داخلية. هذا هو الاستخدام الصحيح لـ
GitHub Actions: جوب قصير يشتغل ثواني، مو جوب طويل يشغل ساعات.

السبب: الحلقة المستمرة (5h45m لكل استدعاء) كانت بتستهلك حصة الدقائق
المجانية (~2000 دقيقة/شهر على private repo) خلال يوم ونص تقريباً لو
اتربطت بـ cron كل 6 ساعات. النمط الحالي (فحص خاطف كل 10-15 دقيقة)
يعطي نفس التغطية الزمنية تقريباً باستهلاك أقل بعشرات المرات.

تغييرات رئيسية عن نسخة الحلقة:
  1. لا يوجد loop داخلي — main() يعمل دورة واحدة (RSS + احتمال Liquipedia)
     ثم يطلع.
  2. Liquipedia يفحص فقط لو مرت مدة كافية منذ آخر فحص محفوظة بالـ state
     (بدل "كل 5 دورات" لأنه ما فيه دورات متعددة بالاستدعاء الواحد).
  3. commit لـ state.json يصير مرة وحدة في نهاية كل استدعاء (بدل كل 4
     دورات) لأن كل استدعاء أصلاً قصير.
  4. جلب كل مصادر RSS بالتوازي (ThreadPoolExecutor) — نفس المنطق القديم،
     محتفظ به لأنه يخلي الاستدعاء الواحد يخلص خلال ثواني بدل دقائق.

Pipeline (once per invocation):
  1. RSS phase: fetch all feeds in feeds.py IN PARALLEL, filter freshness +
     dedup, analyze via Gemini, send.
  2. Liquipedia phase (only if LIQUIPEDIA_MIN_INTERVAL_MINUTES have passed
     since last Liquipedia check): poll watchlist pages, filter
     bot/minor/tiny edits, send.

State is unified in state.json with four collections:
  - urls: seen RSS URLs (ring of last 8000)
  - title_hashes: normalized title hashes (ring of last 8000)
  - liquipedia: per-page seen revids + last seen size
  - last_liquipedia_check: ISO timestamp of last Liquipedia phase run

Configuration sources: feeds.py (RSS_FEEDS), watchlist.py (WATCHLIST).
Secrets: DISCORD_WEBHOOK_URL, GEMINI_API_KEY in environment.

GitHub Actions workflow (run.yml) should trigger this via:
  on:
    workflow_dispatch:
    schedule:
      - cron: "*/15 * * * *"   # every 15 minutes
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

from feeds import RSS_FEEDS
from watchlist import WATCHLIST

# ============================================================
# Configuration
# ============================================================
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()

STATE_FILE = Path("state.json")

# Cap to prevent flooding if many fresh items appear at once in one pass
MAX_MESSAGES_PER_RUN = 50

# Discord webhook rate limit safety margin
MESSAGE_DELAY_SECONDS = 1.0

# RSS freshness window: ignore items older than this
MAX_AGE_HOURS = 24

# State ring sizes
SEEN_URLS_RING = 8000
SEEN_TITLES_RING = 8000
SEEN_REVS_PER_PAGE = 20

# ------------------------------------------------------------
# Single-pass settings
# ------------------------------------------------------------
# Liquipedia is checked at most once every N minutes, tracked via
# state["last_liquipedia_check"], since each invocation is now a single
# short pass rather than one cycle among many inside a long-lived loop.
LIQUIPEDIA_MIN_INTERVAL_MINUTES = 10

# RSS parallel fetch settings
RSS_FETCH_WORKERS = 40
RSS_FETCH_TIMEOUT_SECONDS = 10

# Liquipedia API
LIQUIPEDIA_USER_AGENT = "GGNewsAR Bot/2.0 (https://ggnewsar.com; hazem@ggnewsar.com)"
LIQUIPEDIA_RATE_LIMIT_SEC = 2.5
LIQUIPEDIA_BATCH_SIZE = 50
LIQUIPEDIA_MIN_BYTES_CHANGE = 100  # ignore edits smaller than this

# Discord embed color
EMBED_COLOR = 0x7C3AED
DESC_MAX = 600

# Strip "Source - Article Title" patterns from RSS titles for dedup
SOURCE_SUFFIX_RE = re.compile(r"\s*[\-\|\u2013\u2014:]\s*[^\-\|\u2013\u2014:]{1,40}$")

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
def load_state() -> dict:
    if not STATE_FILE.exists():
        return {
            "urls": [],
            "title_hashes": [],
            "liquipedia": {},  # "wiki:page" -> {"revids": [...], "size": int}
            "last_liquipedia_check": None,  # ISO timestamp string or None
        }
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.error(f"state.json corrupted, starting fresh: {e}")
        return {"urls": [], "title_hashes": [], "liquipedia": {}, "last_liquipedia_check": None}

    data.setdefault("urls", [])
    data.setdefault("title_hashes", [])
    data.setdefault("liquipedia", {})
    data.setdefault("last_liquipedia_check", None)
    return data


def save_state(state: dict) -> None:
    state["urls"] = state["urls"][-SEEN_URLS_RING:]
    state["title_hashes"] = state["title_hashes"][-SEEN_TITLES_RING:]
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def git_commit_push(reason: str = "") -> None:
    """Commit + push state.json if it changed. Safe to call even when
    nothing changed — no-ops cleanly. Never raises: a failed push here
    should not crash the run, just gets retried on the next invocation."""
    try:
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"],
                        check=True, capture_output=True)
        subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"],
                        check=True, capture_output=True)
        subprocess.run(["git", "add", "state.json"], check=True, capture_output=True)

        diff = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if diff.returncode == 0:
            return  # nothing changed, nothing to commit

        msg = "chore: update state.json [skip ci]"
        if reason:
            msg += f" ({reason})"
        subprocess.run(["git", "commit", "-m", msg], check=True, capture_output=True)
        subprocess.run(["git", "pull", "--rebase", "origin", "main"], capture_output=True)

        r = subprocess.run(["git", "push"], capture_output=True, text=True)
        if r.returncode != 0:
            log.warning(f"git push failed: {r.stderr[:300]}")
    except subprocess.CalledProcessError as e:
        log.warning(f"git commit/push step failed: {e}")


# ============================================================
# Discord
# ============================================================
def _clip(text: str, limit: int) -> str:
    if not text:
        return ""
    text = text.strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def send_discord(title: str, link: str = "", source: str = "", summary: str = "", image_url: str = "") -> bool:
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
def normalize_title(title: str) -> str:
    t = title.lower().strip()
    t = SOURCE_SUFFIX_RE.sub("", t).strip()
    t = re.sub(r"[^\w\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def title_hash(title: str) -> str:
    return hashlib.md5(normalize_title(title).encode("utf-8")).hexdigest()


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
    Called from a thread pool so ~159 sources are fetched concurrently
    instead of one-by-one, keeping each single-pass invocation fast
    (seconds, not minutes) even with a slow/dead source mixed in."""
    name = feed_info["name"]
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
    send budget stay predictable. Returns number of messages sent."""
    seen_urls = set(state["urls"])
    seen_titles = set(state["title_hashes"])

    stats = defaultdict(int)
    failed = []
    sent = 0

    log.info(f"RSS phase: {len(RSS_FEEDS)} sources, {RSS_FETCH_WORKERS} parallel workers, freshness={MAX_AGE_HOURS}h")

    fetch_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=RSS_FETCH_WORKERS) as executor:
        futures = {executor.submit(fetch_one_feed, fi): fi for fi in RSS_FEEDS}
        for future in concurrent.futures.as_completed(futures):
            fetch_results.append(future.result())

    for name, entries, error in fetch_results:
        if error:
            stats["sources_failed"] += 1
            failed.append(f"{name}: {error}")
            continue
        stats["sources_ok"] += 1

        for entry in entries:
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

            # Passes all gates. Mark seen regardless of send outcome.
            seen_urls.add(link); state["urls"].append(link)
            seen_titles.add(t_hash); state["title_hashes"].append(t_hash)

            if first_run:
                stats["baseline_recorded"] += 1
                continue

            if sent >= sent_budget:
                stats["skip_cap"] += 1
                state["urls"].pop()
                state["title_hashes"].pop()
                seen_urls.discard(link)
                seen_titles.discard(t_hash)
                continue

            clean_summary = strip_html(summary)

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
            )
            if ok:
                sent += 1
                stats["sent"] += 1
                time.sleep(MESSAGE_DELAY_SECONDS)
            else:
                stats["send_failures"] += 1

    log.info("--- RSS Summary ---")
    for k in sorted(stats.keys()):
        log.info(f"  {k:30s} {stats[k]}")
    if failed:
        log.info(f"--- Failed Sources ({len(failed)}) ---")
        for line in failed:
            log.info(f"  - {line}")

    return sent


# ============================================================
# Liquipedia phase  (unchanged internals — no Gemini analysis here)
# ============================================================
def fetch_liquipedia_revisions(wiki: str, pages: list, session: requests.Session) -> list:
    """Fetch latest revision for each page on a Liquipedia wiki."""
    if not pages:
        return []

    url = f"https://liquipedia.net/{wiki}/api.php"
    all_revs = []

    for i in range(0, len(pages), LIQUIPEDIA_BATCH_SIZE):
        batch = pages[i:i + LIQUIPEDIA_BATCH_SIZE]

        params = {
            "action": "query",
            "format": "json",
            "prop": "revisions",
            "titles": "|".join(batch),
            "rvprop": "ids|timestamp|user|comment|size|flags",
            "maxlag": 5,
            "redirects": 1,
        }

        try:
            time.sleep(LIQUIPEDIA_RATE_LIMIT_SEC)
            r = session.get(url, params=params, timeout=30)

            if r.status_code == 503 or "X-Database-Lag" in r.headers:
                wait = int(r.headers.get("Retry-After", 60))
                log.warning(f"Liquipedia maxlag on {wiki}, waiting {wait}s")
                time.sleep(wait)
                continue

            r.raise_for_status()
            data = r.json()

            if "error" in data:
                log.error(f"Liquipedia API error on {wiki}: {data['error']}")
                continue

            for page_id, page_info in data.get("query", {}).get("pages", {}).items():
                if page_id == "-1" or "missing" in page_info:
                    continue

                page_title = page_info.get("title", "")
                slug = page_title.replace(" ", "_")

                for rev in page_info.get("revisions", []):
                    rev["page_title"] = page_title
                    rev["wiki"] = wiki
                    rev["page_url"] = f"https://liquipedia.net/{wiki}/{slug}"
                    rev["diff_url"] = (
                        f"https://liquipedia.net/{wiki}/index.php?"
                        f"title={slug}&diff={rev['revid']}&oldid={rev.get('parentid', 0)}"
                    )
                    all_revs.append(rev)

        except requests.RequestException as e:
            log.error(f"Liquipedia fetch failed on {wiki}: {e}")
        except ValueError as e:
            log.error(f"Liquipedia JSON parse failed on {wiki}: {e}")

    return all_revs


def is_meaningful_edit(rev: dict, prev_size: int) -> tuple[bool, str]:
    """Structural filter only — no keyword check. Drops bot/minor/tiny edits."""
    user = (rev.get("user") or "").lower()
    new_size = rev.get("size", 0)
    delta = abs(new_size - prev_size) if prev_size else new_size

    if "bot" in user:
        return False, "bot edit"
    if rev.get("minor"):
        return False, "marked minor"
    if delta < LIQUIPEDIA_MIN_BYTES_CHANGE:
        return False, f"tiny change ({delta} bytes)"
    return True, f"{delta} bytes changed"


GAME_NAMES = {
    "counterstrike": "Counter Strike 2", "valorant": "VALORANT",
    "leagueoflegends": "League of Legends", "dota2": "Dota 2",
    "rainbowsix": "Rainbow Six Siege", "rocketleague": "Rocket League",
    "mobilelegends": "Mobile Legends", "honorofkings": "Honor of Kings",
    "pubgmobile": "PUBG Mobile", "fighters": "Fighting Games",
    "easportsfc": "EA Sports FC",
}


def liquipedia_phase(state: dict, first_run: bool, sent_budget: int) -> int:
    """Run Liquipedia collection. Returns number of messages sent."""
    lp_state = state["liquipedia"]
    sent = 0
    stats = defaultdict(int)

    total_pages = sum(len(p) for p in WATCHLIST.values())
    log.info(f"Liquipedia phase: {total_pages} pages across {len(WATCHLIST)} wikis")

    session = requests.Session()
    session.headers.update({
        "User-Agent": LIQUIPEDIA_USER_AGENT,
        "Accept-Encoding": "gzip",
    })

    for wiki, pages in WATCHLIST.items():
        if not pages:
            continue

        revisions = fetch_liquipedia_revisions(wiki, pages, session)
        stats[f"fetched_{wiki}"] = len(revisions)

        for rev in revisions:
            page_key = f"{wiki}:{rev['page_title']}"
            revid = str(rev.get("revid"))

            page_state = lp_state.setdefault(page_key, {"revids": [], "size": 0})

            if revid in page_state["revids"]:
                stats["skip_seen_rev"] += 1
                continue

            page_state["revids"].append(revid)
            page_state["revids"] = page_state["revids"][-SEEN_REVS_PER_PAGE:]

            if first_run:
                page_state["size"] = rev.get("size", 0)
                stats["baseline_recorded"] += 1
                continue

            prev_size = page_state.get("size", 0)
            keep, reason = is_meaningful_edit(rev, prev_size)
            page_state["size"] = rev.get("size", 0)

            if not keep:
                stats[f"drop_{reason.split()[0]}"] += 1
                continue

            if sent >= sent_budget:
                stats["skip_cap"] += 1
                page_state["revids"].pop()
                continue

            game = GAME_NAMES.get(rev["wiki"], rev["wiki"])
            comment = (rev.get("comment") or "").strip()[:200] or "بدون ملاحظة"
            user = rev.get("user") or "?"

            ok = send_discord(
                title=rev["page_title"],
                link=rev["page_url"],
                source=f"Liquipedia · {game} · المحرر: {user}",
                summary=comment,
            )
            if ok:
                sent += 1
                stats["sent"] += 1
                time.sleep(MESSAGE_DELAY_SECONDS)
            else:
                stats["send_failures"] += 1

    log.info("--- Liquipedia Summary ---")
    for k in sorted(stats.keys()):
        log.info(f"  {k:30s} {stats[k]}")

    return sent


def should_run_liquipedia(state: dict) -> bool:
    """True on first run, if no prior check is recorded, or if enough time
    has passed since the last Liquipedia check. Since each invocation is a
    single short pass now (no cycles), this replaces the old 'every 5th
    cycle' rule with a wall-clock interval stored in state."""
    last = state.get("last_liquipedia_check")
    if not last:
        return True
    try:
        last_dt = datetime.fromisoformat(last)
    except ValueError:
        return True
    return (datetime.now(timezone.utc) - last_dt) >= timedelta(minutes=LIQUIPEDIA_MIN_INTERVAL_MINUTES)


# ============================================================
# Main — single pass
# ============================================================
def main():
    if not DISCORD_WEBHOOK_URL:
        log.error("Missing DISCORD_WEBHOOK_URL env var")
        return

    if not GEMINI_API_KEY:
        log.warning("Missing GEMINI_API_KEY — Gemini analysis disabled, will fall back to raw RSS titles/summaries.")

    state = load_state()
    first_run = (
        len(state["urls"]) == 0
        and len(state["title_hashes"]) == 0
        and len(state["liquipedia"]) == 0
    )
    if first_run:
        log.info("FIRST RUN: indexing baseline, no messages will be sent this pass.")

    rss_sent = rss_phase(state, first_run, MAX_MESSAGES_PER_RUN)
    remaining = MAX_MESSAGES_PER_RUN - rss_sent

    lp_sent = 0
    log.info("Liquipedia phase disabled — RSS only.")

    save_state(state)
    git_commit_push("single pass")

    log.info(f"=== Pass done. RSS sent: {rss_sent}, Liquipedia sent: {lp_sent} ===")


if __name__ == "__main__":
    main()
