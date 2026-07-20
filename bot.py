"""
GGNewsAR Discord Bot — RSS-only pipeline, raw (no AI) edition.

مشروع مستقل تماماً عن بوت تيليقرام. الإرسال يروح لروم Discord عبر Webhook.

=== ARCHITECTURE CHANGE (2026-07-20): قناة مستقلة لأخبار الرعايات والبزنس ===
أي خبر يُصنَّف "رعاية/بزنس" (إما لأنه جاء من مصدر مُعلَّم بـ
"category": "sponsorship" في feeds.py، أو لأن عنوانه/ملخصه يحتوي كلمة
مفتاحية من SPONSORSHIP_KEYWORDS أدناه) يروح لقناة Discord مستقلة عبر
SPONSORSHIP_WEBHOOK_URL بدل القناة العامة. لو السيكرت هذا غير موجود
بالبيئة، يرجع تلقائياً يرسل بنفس القناة العامة (DISCORD_WEBHOOK_URL) بدون
أي كسر بالتشغيل. راجع is_sponsorship_news() تحت.

=== ARCHITECTURE CHANGE (2026-07-18): إزالة Liquipedia بالكامل ===
تم حذف مرحلة Liquipedia نهائياً. البوت الحين يعتمد على RSS فقط. ما عاد
يرسل أي تعديلات صفحات من Liquipedia (لا watchlist ولا مراقبة revisions).
ملف watchlist.py صار غير مستخدم من bot.py.

=== ARCHITECTURE CHANGE (2026-07-17, v2): إزالة الترجمة الآلية بالكامل ===
جربنا تمرير كل خبر عبر Gemini للترجمة والتلخيص، لكن قرار حازم إيقاف هذا
تماماً. الحين كل خبر RSS يترسل كما هو بالضبط (العنوان والملخص الأصليين،
بدون أي ترجمة أو استدعاء API خارجي). هذا يلغي أي اعتماد على حصة Gemini
المجانية أو جودة الترجمة، ويسمح برفع سقف عدد الرسائل بكل تشغيلة
(MAX_MESSAGES_PER_RUN) لأن ما فيه قيد API يوقفنا.

تصنيف "الأهمية" (عاجل / مهم / عادي) لسا موجود، لكن الحين مبني بالكامل على
كلمات مفتاحية + قائمة فرق MENA ذات أولوية (بدون أي نموذج ذكاء اصطناعي) —
راجع keyword_floor() و MENA_PRIORITY_ORGS تحت.

=== ARCHITECTURE CHANGE (2026-07-05): single pass ===
كل استدعاء يفحص كل المصادر مرة وحدة ويطلع. الاستمرارية (كل 10-15 دقيقة)
تجيها من GitHub Actions schedule (cron) في run.yml، مو من حلقة داخلية.

Pipeline (once per invocation):
1. RSS phase: fetch all feeds in feeds.py IN PARALLEL, filter freshness +
   dedup, send raw title/summary/image as-is.

State is stored in state.json with these collections:
- urls: seen RSS URLs (ring of last 8000)
- title_hashes: normalized title hashes (ring of last 8000)

Configuration source: feeds.py (RSS_FEEDS).
Secrets: DISCORD_WEBHOOK_URL in environment.

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

from feeds import RSS_FEEDS

# ============================================================
# Configuration
# ============================================================

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()

# Dedicated channel for sponsorship/business news (deals, partnerships,
# investment, acquisitions). Optional: if not set, sponsorship items fall
# back to DISCORD_WEBHOOK_URL automatically — nothing breaks, they just
# won't be split out until this secret is added.
SPONSORSHIP_WEBHOOK_URL = os.environ.get("SPONSORSHIP_WEBHOOK_URL", "").strip()

STATE_FILE = Path("state.json")

# Cap to prevent flooding if many fresh items appear at once in one pass.
# Raised from 50 -> 150 now that there's no AI call per item slowing/
# limiting throughput; the only real constraint left is Discord's own
# rate limit, which send_discord() already backs off for automatically.
MAX_MESSAGES_PER_RUN = 150

# Discord webhook rate limit safety margin
MESSAGE_DELAY_SECONDS = 0.6

# RSS freshness window: ignore items older than this
MAX_AGE_HOURS = 24

# State ring sizes
SEEN_URLS_RING = 8000
SEEN_TITLES_RING = 8000

# RSS parallel fetch settings
RSS_FETCH_WORKERS = 40
RSS_FETCH_TIMEOUT_SECONDS = 10

# Discord embed color
EMBED_COLOR = 0x7C3AED
DESC_MAX = 600

# Strip "Source - Article Title" patterns from RSS titles for dedup
SOURCE_SUFFIX_RE = re.compile(r"\s*[\-\|\u2013\u2014:]\s*[^\-\|\u2013\u2014:]{1,40}$")

IMPORTANCE_LABELS = {"عاجل", "مهم", "عادي"}
IMPORTANCE_RANK = {"عادي": 0, "مهم": 1, "عاجل": 2}

# ------------------------------------------------------------
# Importance tagging — pure keyword/org matching, no AI involved.
# Every item still gets a best-effort "عاجل/مهم/عادي" tag so you can
# triage fast, but it's cheap, instant, and has zero external dependency.
# ------------------------------------------------------------

# MENA/Arab orgs you cover closely. Edit this list any time — any item
# mentioning one of these gets bumped to at least "مهم".
MENA_PRIORITY_ORGS = [
    "Team Falcons", "Falcons",
    "Twisted Minds",
    "Geekay Esports", "Geekay",
    "Nigma Galaxy", "Nigma",
    "Team Vision",
    "PSG Esports",
    "Anubis Gaming",
    "NASR Esports",
]

# Words that, if present (English or Arabic), set a minimum importance
# level for the raw item.
URGENT_KEYWORDS = [
    "wins", "champion", "championship", "signs", "signed", "parts ways",
    "released", "eliminated", "trophy", "grand final", "title win",
    "يفوز", "بطل", "تعاقد", "استغناء", "إقصاء", "لقب", "النهائي",
]
IMPORTANT_KEYWORDS = [
    "roster", "lineup", "transfer", "joins", "benched", "coach", "captain",
    "qualifies", "qualified", "playoffs", "bracket", "announcement",
    "روستر", "تشكيلة", "انتقال", "ينضم", "مدرب", "كابتن", "تأهل", "تأهلت",
]


def keyword_floor(text: str) -> str:
    """Importance implied by raw keywords in the text (any language)."""
    t = (text or "").lower()
    for kw in URGENT_KEYWORDS:
        if kw.lower() in t:
            return "عاجل"
    for kw in IMPORTANT_KEYWORDS:
        if kw.lower() in t:
            return "مهم"
    return "عادي"


def is_priority_org(text: str) -> bool:
    t = (text or "").lower()
    return any(org.lower() in t for org in MENA_PRIORITY_ORGS)


def compute_importance(raw_text: str) -> str:
    """Final importance tag for a raw (untranslated) item: keyword floor,
    bumped to at least "مهم" if a priority MENA org is mentioned."""
    importance = keyword_floor(raw_text)
    if is_priority_org(raw_text) and IMPORTANCE_RANK[importance] < IMPORTANCE_RANK["مهم"]:
        importance = "مهم"
    return importance


# ------------------------------------------------------------
# Sponsorship / business routing — pure source-tag + keyword matching,
# no AI involved. Any source tagged "category": "sponsorship" in
# feeds.py routes there automatically; any item from a GENERAL source
# that happens to be a sponsorship/deal/investment story also gets
# caught by the keyword scan below and redirected the same way, so a
# business story doesn't get stuck in the general news channel just
# because it came from HLTV/Dot Esports/etc. instead of a business-only
# source.
# ------------------------------------------------------------

SPONSORSHIP_SOURCE_NAMES = {
    f["name"] for f in RSS_FEEDS if f.get("category") == "sponsorship"
}

SPONSORSHIP_KEYWORDS = [
    # English
    "sponsor", "sponsors", "sponsored", "sponsorship",
    "partners with", "partnership", "multi-year deal", "multi-year partnership",
    "signs deal", "signs sponsorship", "signs partnership", "naming rights",
    "title sponsor", "jersey sponsor", "brand deal", "brand partnership",
    "investment", "invests in", "invests", "funding round", "raises $",
    "raises funding", "series a", "series b", "acquisition", "acquires",
    "acquired", "stake in", "valuation", "ipo", "revenue", "franchise slot",
    "media rights", "broadcast deal", "streaming deal", "front office",
    # Arabic
    "رعاية", "راعي", "يرعى", "شراكة", "يستثمر", "استثمار",
    "تمويل", "جولة تمويل", "استحواذ", "يستحوذ", "حصة", "صفقة",
    "عقد رعاية", "حقوق البث", "حقوق تسمية",
]


def is_sponsorship_news(source_name: str, raw_text: str) -> bool:
    """True if this item belongs in the sponsorship/business channel:
    either the source itself is a dedicated business source, or the
    title/summary text matches a sponsorship/deal/investment keyword."""
    if source_name in SPONSORSHIP_SOURCE_NAMES:
        return True
    t = (raw_text or "").lower()
    return any(kw.lower() in t for kw in SPONSORSHIP_KEYWORDS)


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
        }
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.error(f"state.json corrupted, starting fresh: {e}")
        return {"urls": [], "title_hashes": []}
    data.setdefault("urls", [])
    data.setdefault("title_hashes", [])
    # Drop any leftover Liquipedia state from older versions of the bot.
    data.pop("liquipedia", None)
    data.pop("last_liquipedia_check", None)
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


def send_discord(title: str, link: str = "", source: str = "", summary: str = "",
                  image_url: str = "", importance: str = "", webhook_url: str = "") -> bool:
    """Send one news item to Discord as an embed. Returns True on success.
    webhook_url lets callers route to a specific channel (e.g. the
    sponsorship channel); if omitted, falls back to the main
    DISCORD_WEBHOOK_URL."""
    target = webhook_url or DISCORD_WEBHOOK_URL
    if not target:
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
    if importance:
        embed["fields"] = [{"name": "الأهمية", "value": importance, "inline": True}]

    payload = {"embeds": [embed]}
    for attempt in range(3):
        try:
            r = requests.post(target, json=payload, timeout=15)
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
    Called from a thread pool so all sources are fetched concurrently
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
    """Run RSS collection. Fetches all sources in parallel, dedups, then
    sends each eligible item AS-IS (raw title + raw summary, no
    translation/analysis). Returns number of messages sent."""
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
                continue

            clean_summary = strip_html(summary)
            image = extract_image(entry)
            combined_text = f"{title} {clean_summary}"
            importance = compute_importance(combined_text)
            sponsorship = is_sponsorship_news(name, combined_text)
            target_webhook = SPONSORSHIP_WEBHOOK_URL if (sponsorship and SPONSORSHIP_WEBHOOK_URL) else DISCORD_WEBHOOK_URL

            ok = send_discord(
                title=title,
                link=link,
                source=name,
                summary=clean_summary[:DESC_MAX],
                image_url=image,
                importance=importance,
                webhook_url=target_webhook,
            )
            if ok:
                sent += 1
                stats["sent"] += 1
                stats["sent_sponsorship" if sponsorship else "sent_general"] += 1
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
# Main — single pass
# ============================================================

def main():
    if not DISCORD_WEBHOOK_URL:
        log.error("Missing DISCORD_WEBHOOK_URL env var")
        return
    if SPONSORSHIP_WEBHOOK_URL:
        log.info("Sponsorship/business channel routing ENABLED (SPONSORSHIP_WEBHOOK_URL set).")
    else:
        log.info("SPONSORSHIP_WEBHOOK_URL not set — sponsorship items will fall back to the main channel.")

    state = load_state()
    first_run = (
        len(state["urls"]) == 0
        and len(state["title_hashes"]) == 0
    )
    if first_run:
        log.info("FIRST RUN: indexing baseline, no messages will be sent this pass.")

    rss_sent = rss_phase(state, first_run, MAX_MESSAGES_PER_RUN)

    save_state(state)
    git_commit_push("single pass, raw content")

    log.info(f"=== Pass done. RSS sent: {rss_sent} ===")


if __name__ == "__main__":
    main()
