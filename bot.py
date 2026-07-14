"""
GGNewsAR Discord Bot — unified RSS + Liquipedia pipeline (single-pass edition).

مشروع مستقل تماماً عن بوت تيليقرام. نفس المنطق بالضبط (RSS + Liquipedia +
dedup + state)، لكن الإرسال يروح لروم Discord عبر Webhook بدل تيليقرام.

=== تحديث (2026-07-07): إلغاء تحليل Gemini ===
البوت يرسل الآن عنوان/ملخص RSS الخام مباشرة بدون أي تحليل أو ترجمة أو
تصنيف عبر Gemini. القرار: السرعة واكتمال الخبر أولوية، وأي طبقة تحليل
إضافية (حتى لو موازية) تبقى نقطة فشل محتملة وتأخير غير ضروري. الخبر يوصل
لروم Discord فور اكتشافه بنفس دورة الفحص، بدون أي انتظار.

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
     dedup, send raw title/summary immediately (no analysis step).
  2. Liquipedia phase (only if LIQUIPEDIA_MIN_INTERVAL_MINUTES have passed
     since last Liquipedia check): poll watchlist pages, filter
     bot/minor/tiny edits, send.

State is unified in state.json with four collections:
  - urls: seen RSS URLs (ring of last 8000)
  - title_hashes: normalized title hashes (ring of last 8000)
  - liquipedia: per-page seen revids + last seen size
  - last_liquipedia_check: ISO timestamp of last Liquipedia phase run

Configuration sources: feeds.py (RSS_FEEDS), watchlist.py (WATCHLIST).
Secrets: DISCORD_WEBHOOK_URL in environment.

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
from transfers import (
    TRANSFER_WIKIS,
    TRANSFER_PAGE_PATTERNS,
    TRANSFERS_MODE,
    TRANSFERS_MAX_PER_RUN,
    TRANSFERS_MIN_INTERVAL_MINUTES,
    extract_transfer_templates,
    parse_transfer,
    classify,
    row_key,
    format_headline,
)

# ============================================================
# Configuration
# ============================================================
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()

# Optional: a dedicated #transfers channel. If unset, transfer messages go
# to the main webhook alongside the news feed.
TRANSFERS_WEBHOOK_URL = (
    os.environ.get("TRANSFERS_WEBHOOK_URL", "").strip() or DISCORD_WEBHOOK_URL
)

# Distinct embed colour so transfers are visually separable from news
TRANSFER_EMBED_COLOR = 0x16A34A   # green
MENA_EMBED_COLOR = 0xF59E0B       # amber — Arab/MENA org involved

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
SEEN_TRANSFERS_RING = 6000

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
# Discord embed description hard limit is 4096 chars — use nearly all of it
# since summaries are now sent raw/complete (no Gemini summarization step).
DESC_MAX = 4000

# Strip "Source - Article Title" patterns from RSS titles for dedup
SOURCE_SUFFIX_RE = re.compile(r"\s*[\-\|\u2013\u2014:]\s*[^\-\|\u2013\u2014:]{1,40}$")

# ------------------------------------------------------------


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
            "transfers": {"pages": {}, "seen": [], "last_check": None},
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
    data.setdefault("transfers", {"pages": {}, "seen": [], "last_check": None})
    data["transfers"].setdefault("pages", {})
    data["transfers"].setdefault("seen", [])
    data["transfers"].setdefault("last_check", None)
    return data


def save_state(state: dict) -> None:
    state["urls"] = state["urls"][-SEEN_URLS_RING:]
    state["title_hashes"] = state["title_hashes"][-SEEN_TITLES_RING:]
    if "transfers" in state:
        state["transfers"]["seen"] = state["transfers"]["seen"][-SEEN_TRANSFERS_RING:]
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


def _build_embed(title: str, link: str = "", source: str = "", summary: str = "", image_url: str = "", color: int = None) -> dict:
    embed = {
        "title": _clip(title, 256),
        "color": color if color is not None else EMBED_COLOR,
    }
    if link:
        embed["url"] = link
    if summary:
        embed["description"] = _clip(summary, DESC_MAX)
    if source:
        embed["footer"] = {"text": _clip(source, 2048)}
    if image_url:
        embed["image"] = {"url": image_url}
    return embed


def send_discord(title: str, link: str = "", source: str = "", summary: str = "", image_url: str = "",
                 webhook: str = "", color: int = None) -> bool:
    """Send one news item to Discord as an embed. Returns True on success."""
    hook = webhook or DISCORD_WEBHOOK_URL
    if not hook:
        log.error("Discord webhook missing")
        return False

    payload = {"embeds": [_build_embed(title, link, source, summary, image_url, color)]}

    for attempt in range(3):
        try:
            r = requests.post(hook, json=payload, timeout=15)
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


# Source-level priority (set per feed in feeds.py via "priority": "high").
# Feeds with no "priority" key default to "normal". High-priority sources
# (e.g. official team/organizer X accounts bridged through RSSHub) get
# processed and sent before normal sources in the same pass.
FEED_PRIORITY = {fi["name"]: fi.get("priority", "normal") for fi in RSS_FEEDS}
PRIORITY_RANK = {"high": 0, "normal": 1, "low": 2}


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


def entry_published_dt(entry):
    """Best-effort publish datetime for latency measurement. None if unknown."""
    pub = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if not pub:
        return None
    try:
        return datetime(*pub[:6], tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def rss_phase(state: dict, first_run: bool, sent_budget: int) -> int:
    """Run RSS collection in two passes:

    Pass 1 — fetch all sources in parallel, apply freshness/dedup gates,
    and collect every item that should be sent into a candidate list
    (no sending yet).

    Pass 2 — sort candidates by source priority (feeds.py "priority": "high"
    go first, e.g. official team/organizer accounts) so the most important
    sources get first claim on the per-run send budget, then send the raw
    RSS title/summary immediately, in that order. Latency (publish time ->
    send time) is logged per item and averaged in the summary so slow
    sources are visible.
    """
    seen_urls = set(state["urls"])
    seen_titles = set(state["title_hashes"])

    stats = defaultdict(int)
    failed = []
    sent = 0
    latencies = []

    log.info(f"RSS phase: {len(RSS_FEEDS)} sources, {RSS_FETCH_WORKERS} parallel workers, freshness={MAX_AGE_HOURS}h")

    fetch_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=RSS_FETCH_WORKERS) as executor:
        futures = {executor.submit(fetch_one_feed, fi): fi for fi in RSS_FEEDS}
        for future in concurrent.futures.as_completed(futures):
            fetch_results.append(future.result())

    # --- Pass 1: gate + collect candidates (no sending yet) ---
    candidates = []
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

            candidates.append({
                "name": name,
                "priority": FEED_PRIORITY.get(name, "normal"),
                "entry": entry,
                "link": link,
                "title": title,
                "summary": summary,
                "t_hash": t_hash,
                "published_dt": entry_published_dt(entry),
            })

    # --- Pass 2: sort by source priority, trim to budget, send raw RSS
    # title/summary to Discord immediately. No analysis step of any kind —
    # this is intentional: fewer moving parts means fewer ways for a news
    # item to be delayed, altered, or dropped. Sending is sequential with
    # MESSAGE_DELAY_SECONDS between messages to stay under Discord's
    # webhook rate limit.
    candidates.sort(key=lambda c: PRIORITY_RANK.get(c["priority"], 1))

    to_process = candidates[:sent_budget]
    overflow = candidates[sent_budget:]
    for cand in overflow:
        stats["skip_cap"] += 1
        if cand["link"] in state["urls"]:
            state["urls"].remove(cand["link"])
        if cand["t_hash"] in state["title_hashes"]:
            state["title_hashes"].remove(cand["t_hash"])

    for cand in to_process:
        entry = cand["entry"]
        title = cand["title"]
        link = cand["link"]
        name = cand["name"]
        clean_summary = strip_html(cand["summary"])

        ok = send_discord(
            title=title,
            link=link,
            source=name,
            summary=clean_summary,
            image_url=extract_image(entry),
        )
        if ok:
            sent += 1
            stats["sent"] += 1
            if cand["published_dt"]:
                lag = (datetime.now(timezone.utc) - cand["published_dt"]).total_seconds()
                latencies.append(lag)
                log.info(f"  sent '{title[:60]}' — latency {lag/60:.1f} min (source: {name})")
            time.sleep(MESSAGE_DELAY_SECONDS)
        else:
            stats["send_failures"] += 1

    log.info("--- RSS Summary ---")
    for k in sorted(stats.keys()):
        log.info(f"  {k:30s} {stats[k]}")
    if latencies:
        avg_min = sum(latencies) / len(latencies) / 60
        max_min = max(latencies) / 60
        log.info(f"  {'avg_latency_min':30s} {avg_min:.1f}")
        log.info(f"  {'max_latency_min':30s} {max_min:.1f}")
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
# Transfers phase — Liquipedia Player Transfers pages
# ============================================================
# This is the source of truth for roster moves. Reading it directly means
# GGNewsAR gets the move when it is logged, not when someone writes it up
# hours later. Bench moves, coach changes, stand-ins and releases never
# become RSS articles at all, so this phase is the ONLY way they arrive.
MONTHS = ["January", "February", "March", "April", "May", "June", "July",
          "August", "September", "October", "November", "December"]


def _lp_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": LIQUIPEDIA_USER_AGENT,
        "Accept-Encoding": "gzip",
    })
    return s


def _lp_get(session, wiki: str, params: dict) -> dict:
    """One rate-limited Liquipedia API call. Returns {} on failure."""
    url = f"https://liquipedia.net/{wiki}/api.php"
    params = {**params, "format": "json", "maxlag": 5}
    try:
        time.sleep(LIQUIPEDIA_RATE_LIMIT_SEC)
        r = session.get(url, params=params, timeout=30)
        if r.status_code != 200:
            log.warning(f"Liquipedia {wiki} HTTP {r.status_code}")
            return {}
        return r.json()
    except (requests.RequestException, ValueError) as e:
        log.warning(f"Liquipedia {wiki} request failed: {e}")
        return {}


def resolve_transfer_page(session, wiki: str) -> str:
    """Find the current transfer page title for this wiki.

    Tries the monthly page first, then quarterly, then the yearly page.
    Returns "" if the wiki has no transfer page under any known pattern
    (that is fine — it just gets skipped).
    """
    now = datetime.now(timezone.utc)
    quarter = ["1st", "2nd", "3rd", "4th"][(now.month - 1) // 3]
    candidates = [
        pat.format(y=now.year, m=MONTHS[now.month - 1], q=quarter)
        for pat in TRANSFER_PAGE_PATTERNS
    ]

    data = _lp_get(session, wiki, {
        "action": "query",
        "titles": "|".join(candidates),
        "prop": "info",
        "redirects": 1,
    })
    pages = (data.get("query", {}) or {}).get("pages", {}) or {}
    existing = {
        p.get("title", "").replace(" ", "_")
        for pid, p in pages.items()
        if int(pid) > 0 and "missing" not in p
    }
    for cand in candidates:                 # keep pattern preference order
        if cand in existing:
            return cand
    return ""


def fetch_page_revid(session, wiki: str, title: str) -> int:
    data = _lp_get(session, wiki, {
        "action": "query", "titles": title,
        "prop": "revisions", "rvprop": "ids",
    })
    pages = (data.get("query", {}) or {}).get("pages", {}) or {}
    for pid, p in pages.items():
        revs = p.get("revisions") or []
        if revs:
            return int(revs[0].get("revid", 0))
    return 0


def fetch_page_wikitext(session, wiki: str, title: str) -> str:
    data = _lp_get(session, wiki, {
        "action": "query", "titles": title,
        "prop": "revisions", "rvprop": "content", "rvslots": "main",
    })
    pages = (data.get("query", {}) or {}).get("pages", {}) or {}
    for pid, p in pages.items():
        revs = p.get("revisions") or []
        if not revs:
            continue
        slots = revs[0].get("slots", {})
        if slots:
            return slots.get("main", {}).get("*", "") or ""
        return revs[0].get("*", "") or ""
    return ""


def should_run_transfers(state: dict) -> bool:
    last = state["transfers"].get("last_check")
    if not last:
        return True
    try:
        last_dt = datetime.fromisoformat(last)
    except ValueError:
        return True
    return (datetime.now(timezone.utc) - last_dt) >= timedelta(minutes=TRANSFERS_MIN_INTERVAL_MINUTES)


def transfers_phase(state: dict, first_run: bool, sent_budget: int) -> int:
    """Poll every transfer page, send new rows. Returns messages sent."""
    tstate = state["transfers"]
    seen = set(tstate["seen"])
    stats = defaultdict(int)
    sent = 0
    budget = min(sent_budget, TRANSFERS_MAX_PER_RUN)

    log.info(f"Transfers phase: {len(TRANSFER_WIKIS)} wikis, mode={TRANSFERS_MODE}, budget={budget}")
    session = _lp_session()
    candidates = []

    for wiki, game in TRANSFER_WIKIS.items():
        entry = tstate["pages"].get(wiki, {})
        title = entry.get("title", "")
        month_tag = datetime.now(timezone.utc).strftime("%Y-%m")

        # Re-resolve the page whenever the month rolls over (or first time)
        if not title or entry.get("month") != month_tag:
            title = resolve_transfer_page(session, wiki)
            if not title:
                stats["wikis_no_page"] += 1
                tstate["pages"][wiki] = {"title": "", "month": month_tag, "revid": 0}
                continue
            entry = {"title": title, "month": month_tag, "revid": 0}
            tstate["pages"][wiki] = entry
            log.info(f"  {wiki}: transfer page -> {title}")

        # Cheap change check: skip the page entirely if nothing was edited
        revid = fetch_page_revid(session, wiki, title)
        if revid and revid == entry.get("revid"):
            stats["wikis_unchanged"] += 1
            continue

        wikitext = fetch_page_wikitext(session, wiki, title)
        if not wikitext:
            stats["wikis_empty"] += 1
            continue

        entry["revid"] = revid
        stats["wikis_scanned"] += 1

        for tpl in extract_transfer_templates(wikitext):
            row = parse_transfer(tpl)
            if not row.get("players"):
                continue
            stats["rows_total"] += 1

            key = row_key(wiki, row)
            if key in seen:
                stats["rows_seen"] += 1
                continue
            seen.add(key)
            tstate["seen"].append(key)

            if first_run:
                stats["rows_baseline"] += 1
                continue

            send, tag = classify(row)
            if not send:
                stats["rows_filtered_out"] += 1
                continue

            candidates.append({"wiki": wiki, "game": game, "row": row, "tag": tag})

    # MENA rows first — if the budget is tight, they are the ones that must
    # get through. Then newest date first.
    candidates.sort(key=lambda c: (0 if c["tag"] == "MENA" else 1,
                                   c["row"].get("date", "")), reverse=False)
    to_send = candidates[:budget]
    stats["rows_over_cap"] = max(0, len(candidates) - len(to_send))

    for c in to_send:
        row, game, tag = c["row"], c["game"], c["tag"]
        headline = format_headline(game, row)
        if tag == "MENA":
            headline = f"[MENA] {headline}"

        bits = []
        if row.get("date"):
            bits.append(f"**التاريخ:** {row['date']}")
        bits.append(f"**من:** {row.get('from') or 'Free Agent'}"
                    + (f" ({row['role_from']})" if row.get("role_from") else ""))
        bits.append(f"**إلى:** {row.get('to') or 'Free Agent'}"
                    + (f" ({row['role_to']})" if row.get("role_to") else ""))
        page_url = f"https://liquipedia.net/{c['wiki']}/{tstate['pages'][c['wiki']]['title']}"

        ok = send_discord(
            title=headline,
            link=page_url,
            source=f"Liquipedia Transfers · {game}",
            summary="\n".join(bits),
            webhook=TRANSFERS_WEBHOOK_URL,
            color=MENA_EMBED_COLOR if tag == "MENA" else TRANSFER_EMBED_COLOR,
        )
        if ok:
            sent += 1
            stats["sent"] += 1
            if tag == "MENA":
                stats["sent_mena"] += 1
        else:
            stats["send_failures"] += 1
        time.sleep(MESSAGE_DELAY_SECONDS)

    tstate["last_check"] = datetime.now(timezone.utc).isoformat()

    log.info("--- Transfers Summary ---")
    for k in sorted(stats.keys()):
        log.info(f"  {k:22s} {stats[k]}")

    return sent


# ============================================================
# Main — single pass
# ============================================================
def main():
    if not DISCORD_WEBHOOK_URL:
        log.error("Missing DISCORD_WEBHOOK_URL env var")
        return

    state = load_state()
    first_run = (
        len(state["urls"]) == 0
        and len(state["title_hashes"]) == 0
        and len(state["liquipedia"]) == 0
    )
    # A pre-existing state.json with no transfer history means the transfers
    # feature is new: baseline it silently instead of dumping a whole month
    # of back-catalogue moves into Discord on the first run.
    transfers_first_run = first_run or len(state["transfers"]["seen"]) == 0
    if first_run:
        log.info("FIRST RUN: indexing baseline, no messages will be sent this pass.")

    rss_sent = rss_phase(state, first_run, MAX_MESSAGES_PER_RUN)
    remaining = MAX_MESSAGES_PER_RUN - rss_sent

    lp_sent = 0
    log.info("Liquipedia watchlist phase disabled — RSS + Transfers only.")

    # Transfers get their OWN budget, deliberately not shared with RSS.
    # A busy news day must never be allowed to starve the transfer feed —
    # transfers are the highest-value stream GGNewsAR publishes.
    tr_sent = 0
    if should_run_transfers(state):
        tr_sent = transfers_phase(state, transfers_first_run, TRANSFERS_MAX_PER_RUN)
    else:
        log.info(f"Transfers phase skipped (interval < {TRANSFERS_MIN_INTERVAL_MINUTES} min).")

    save_state(state)
    git_commit_push("single pass")

    log.info(f"=== Pass done. RSS: {rss_sent}, Liquipedia: {lp_sent}, Transfers: {tr_sent} ===")


if __name__ == "__main__":
    main()
