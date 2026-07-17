"""
GGNewsAR Discord Bot — unified RSS + Liquipedia pipeline (AI-for-everything edition).

مشروع مستقل تماماً عن بوت تيليقرام. نفس المنطق بالضبط (RSS + Liquipedia +
dedup + state)، لكن الإرسال يروح لروم Discord عبر Webhook بدل تيليقرام.

كل خبر RSS وكل تعديل Liquipedia يمر أولاً على Gemini (مباشرة عبر Google AI
Studio) اللي يترجمه ويلخصه بالفصحى البيضاء حسب ستايل GGNewsAR، ويطلع معه
تصنيف أهمية (عاجل / مهم / عادي) عشان تقدر تفرز بسرعة وش يستاهل وقتك الحين.
لو التحليل فشل أو تجاوزنا الحصة اليومية المجانية، يرجع البوت تلقائياً للنص
الأصلي مع وسم يوضح إنه غير مُحلَّل.

=== ARCHITECTURE CHANGE (2026-07-17): AI للجميع + batching + حصة يومية ===
1. مرحلة Liquipedia صارت تمر بنفس محرك Gemini اللي تمر فيه RSS (كانت قبل
   كذا توصل خام بالإنجليزي بدون أي ترجمة أو تلخيص).
2. بدل استدعاء Gemini مرة لكل خبر، صرنا نجمع عدة عناصر (GEMINI_BATCH_SIZE)
   بطلب واحد، عشان نقلل عدد الطلبات اليومية بشكل كبير ونحمي الحصة المجانية.
3. عداد حصة يومي محفوظ بـ state.json (gemini_quota: {date, calls}). لو
   قاربنا الحد (GEMINI_DAILY_QUOTA)، نوقف استدعاء Gemini تلقائياً لبقية
   اليوم بدل ما نضرب أخطاء 429 بلا فايدة، ونرسل نسخة احتياطية بدل التحليل.
4. الموديل الافتراضي صار gemini-2.5-flash-lite (حصة يومية أعلى من Flash
   العادي)، قابل للتغيير عبر GEMINI_MODEL لو احتجت جودة أعلى لاحقاً.

=== ARCHITECTURE CHANGE (2026-07-05): single pass ===
كل استدعاء يفحص كل المصادر مرة وحدة ويطلع. الاستمرارية (كل 10-15 دقيقة)
تجيها من GitHub Actions schedule (cron) في run.yml، مو من حلقة داخلية.

Pipeline (once per invocation):
1. RSS phase: fetch all feeds in feeds.py IN PARALLEL, filter freshness +
   dedup, collect eligible items, analyze via Gemini IN BATCHES, send.
2. Liquipedia phase (only if LIQUIPEDIA_MIN_INTERVAL_MINUTES have passed
   since last Liquipedia check): poll watchlist pages, filter
   bot/minor/tiny edits, collect meaningful edits, analyze via Gemini IN
   BATCHES, send.

State is unified in state.json with these collections:
- urls: seen RSS URLs (ring of last 8000)
- title_hashes: normalized title hashes (ring of last 8000)
- liquipedia: per-page seen revids + last seen size
- last_liquipedia_check: ISO timestamp of last Liquipedia phase run
- gemini_quota: {"date": "YYYY-MM-DD", "calls": int} daily call counter

Configuration sources: feeds.py (RSS_FEEDS), watchlist.py (WATCHLIST).
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
# Gemini (direct via Google AI Studio) — translation + summary + importance
# ------------------------------------------------------------
# Flash-Lite carries a noticeably higher free daily quota than full Flash,
# which matters here because EVERY item (RSS + Liquipedia edits) now goes
# through the model instead of just RSS. Override via env var if you ever
# want to trade quota for quality.
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash-lite").strip()
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
GEMINI_TIMEOUT_SECONDS = 30
GEMINI_MAX_RETRIES = 2
GEMINI_CALL_DELAY_SECONDS = 1.0  # spacing between batch calls, RPM safety

# How many items go into a single Gemini call. Fewer calls/day = safer
# against the daily quota; keep low enough that output tokens stay sane.
GEMINI_BATCH_SIZE = int(os.environ.get("GEMINI_BATCH_SIZE", "6"))
GEMINI_TOKENS_PER_ITEM = 300
GEMINI_TOKENS_OVERHEAD = 200

# Daily call counter safety margin. Set conservatively below the published
# free-tier ceiling so we stop BEFORE hitting 429s, not after.
GEMINI_DAILY_QUOTA = int(os.environ.get("GEMINI_DAILY_QUOTA", "900"))

NO_ANALYSIS_NOTE = "⚠ لم تتم الترجمة الآلية لهذا الخبر (تجاوزنا الحصة اليومية المجانية أو فشل التحليل). النص الأصلي أدناه:"

IMPORTANCE_LABELS = {"عاجل", "مهم", "عادي"}
IMPORTANCE_RANK = {"عادي": 0, "مهم": 1, "عاجل": 2}

# ------------------------------------------------------------
# Importance safety net — the model's judgment alone can miss things
# (vague edit comments, no notion of which orgs matter to you). These two
# layers act as a FLOOR on top of whatever Gemini decides: they can only
# raise the importance, never lower it below the model's own call.
# ------------------------------------------------------------

# MENA/Arab orgs you cover closely. Edit this list any time — any item
# mentioning one of these gets bumped to at least "مهم" regardless of what
# the model or keyword floor decided.
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

# Words that, if present (English or Arabic, raw or translated), force a
# minimum importance level even if the model under-rated the item. This
# guards against a big story slipping through as "عادي" because the raw
# edit comment or RSS snippet was too terse for the model to judge well.
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
    """Minimum importance implied by raw keywords in the text (any language)."""
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


def apply_importance_floor(model_importance: str, raw_text: str) -> str:
    """Combine the model's importance call with the keyword/org floors and
    return whichever is highest. Never lowers what the model decided."""
    candidates = [model_importance or "عادي", keyword_floor(raw_text)]
    if is_priority_org(raw_text):
        candidates.append("مهم")
    return max(candidates, key=lambda x: IMPORTANCE_RANK.get(x, 0))

GEMINI_SYSTEM_PROMPT = """أنت محرر أخبار إسبورت لمنصة GGNewsAR، تكتب بالعربية الفصحى البيضاء (لغة يومية مثقفة، مو لغة أدبية أو مترجمة حرفياً).

راح توصلك دفعة عناصر (items) بصيغة JSON. كل عنصر له "id" و"kind":
- kind = "news": خبر RSS، فيه "title" (عنوان أصلي، غالباً إنجليزي) و"context" (ملخص/مقتطف الخبر).
- kind = "wiki_edit": تعديل صفحة على Liquipedia، فيه "title" (اسم الصفحة) و"context" (معلومات: اللعبة، اسم المحرر، وتعليق التعديل الخام غالباً إنجليزي مختصر جداً).

مهمتك لكل عنصر: ترجمة/تحليل المحتوى وإخراج أربعة عناصر: عنوان رئيسي، عنوان فرعي، ملخص قصير، وتصنيف أهمية.

قواعد صارمة تنطبق على كل عنصر:
- العنوان الرئيسي: يحتوي اسم اللعبة إن وجد، يبدأ بأهم معلومة، وينتهي بعلامة استفهام أو تعجب حسب نوع الخبر.
- العنوان الفرعي: جملة واحدة قصيرة تضيف تفصيل أو سياق إضافي، مش تكرار للعنوان الرئيسي.
- الملخص: جملتين أو ثلاث قصيرة ومتتالية، تبدأ بفعل مباشر (تأهل، حسم، أنهى، خطف، عدّل)، بدون نقاط أو عناوين فرعية. لعناصر kind="wiki_edit" اشرح بوضوح إيش تغيّر بالصفحة وليش قد يهمّ (تغيير روستر، نتيجة ماتش، تعاقد، معلومة جديدة... إلخ)، لا تكتفِ بترجمة حرفية للتعليق الخام لو كان غامضاً — استنتج المعنى من اسم الصفحة والسياق المعطى.
- تصنيف الأهمية: قيمة واحدة فقط من ["عاجل", "مهم", "عادي"]. "عاجل" للأخبار الكبيرة (نتائج نهائيات، تعاقدات/انتقالات مؤكدة، انسحابات، قرارات رسمية كبرى). "مهم" لتحديثات مفيدة لكن غير طارئة (نتائج جولات عادية، تفاصيل تنظيمية، تعديلات روستر ثانوية). "عادي" لتعديلات نصية/تدقيقية بسيطة أو أخبار خلفية لا تستدعي أولوية.
- ممنوع أي عبارات حشو مثل: "يأتي ذلك في إطار"، "في خطوة لافتة"، "يُعد علامة فارقة"، "تجدر الإشارة إلى"، "من الجدير بالذكر"، "وفي سياق متصل"، "يُشكل نقلة نوعية"، وصفات فارغة مثل "كبيرة" أو "بارزة" بدون وزن فعلي.
- أسماء اللاعبين تبقى بالإنجليزية كما هي: اللقب فقط (Nickname)، بدون الاسم الحقيقي الكامل (لا Nikola "NiKo" Kovač، فقط NiKo).
- أسماء الفرق والمنظمات والبطولات وأسماء الألعاب تبقى بالإنجليزية كما هي (Team Falcons, IEM Cologne, CS2...)، لا تُعرَّب أبداً. أسماء المدن والدول تُكتب بالعربية.
- الأرقام المالية: أرقام كاملة مع فواصل الآلاف (مثال: 1,000,000)، ما تكتبها بالحروف.
- لو المعطيات ما فيها معلومات كافية لتأكيد تفصيل معين، لا تختلقه — التزم بما هو مذكور فقط.

رد بصيغة JSON فقط، بدون أي نص أو شرح إضافي قبله أو بعده، بالشكل التالي بالضبط:
{"items": [{"id": "...", "headline": "...", "subheadline": "...", "summary": "...", "importance": "..."}]}
لازم عدد العناصر بالرد يطابق عدد العناصر المُرسلة بالضبط، بنفس قيم "id"."""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ggnewsar-discord")


def batch_analyze_with_gemini(items: list) -> dict:
    """Analyze a batch of items (news or wiki_edit) via Gemini in ONE call.

    items: list of {"id": str, "kind": "news"|"wiki_edit", "title": str, "context": str}
    Returns: dict mapping id -> {"headline","subheadline","summary","importance"}
    for items the model successfully analyzed. Missing ids mean fallback
    to raw content should be used by the caller. Returns {} entirely on
    hard failure (network, parse error, etc.) — never raises.
    """
    if not GEMINI_API_KEY or not items:
        return {}

    user_payload = {
        "items": [
            {
                "id": it["id"],
                "kind": it["kind"],
                "title": it["title"],
                "context": it.get("context") or "غير متوفر",
            }
            for it in items
        ]
    }
    max_tokens = GEMINI_TOKENS_OVERHEAD + GEMINI_TOKENS_PER_ITEM * len(items)

    payload = {
        "system_instruction": {"parts": [{"text": GEMINI_SYSTEM_PROMPT}]},
        "contents": [{"role": "user", "parts": [{"text": json.dumps(user_payload, ensure_ascii=False)}]}],
        "generationConfig": {
            "temperature": 0.4,
            "maxOutputTokens": max_tokens,
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
            r = requests.post(GEMINI_URL, json=payload, headers=headers, timeout=GEMINI_TIMEOUT_SECONDS)
            if r.status_code == 429:
                log.warning("Gemini 429 (rate limited), backing off")
                time.sleep(3)
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
                log.warning(f"Gemini returned empty content (attempt {attempt + 1}/{GEMINI_MAX_RETRIES})")
                time.sleep(1)
                continue
            content = content.strip()
            content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.MULTILINE).strip()
            parsed = json.loads(content)
            results = {}
            for entry in parsed.get("items", []):
                eid = entry.get("id")
                if not eid:
                    continue
                if all(k in entry and entry[k] for k in ("headline", "subheadline", "summary", "importance")):
                    imp = entry["importance"].strip()
                    if imp not in IMPORTANCE_LABELS:
                        imp = "عادي"
                    results[eid] = {
                        "headline": entry["headline"],
                        "subheadline": entry["subheadline"],
                        "summary": entry["summary"],
                        "importance": imp,
                    }
            return results
        except (requests.RequestException, ValueError, KeyError, AttributeError, json.JSONDecodeError) as e:
            log.warning(f"Gemini batch analysis failed (attempt {attempt + 1}/{GEMINI_MAX_RETRIES}): {e}")
            time.sleep(1)
    return {}


def chunked(seq: list, size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


# ============================================================
# Gemini daily quota tracking (persisted in state.json)
# ============================================================

def _today_str() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def get_quota_remaining(state: dict) -> int:
    q = state.setdefault("gemini_quota", {"date": _today_str(), "calls": 0})
    if q.get("date") != _today_str():
        q["date"] = _today_str()
        q["calls"] = 0
    return max(0, GEMINI_DAILY_QUOTA - q.get("calls", 0))


def record_gemini_calls(state: dict, n: int) -> None:
    q = state.setdefault("gemini_quota", {"date": _today_str(), "calls": 0})
    if q.get("date") != _today_str():
        q["date"] = _today_str()
        q["calls"] = 0
    q["calls"] = q.get("calls", 0) + n


def analyze_items_in_batches(state: dict, items: list) -> dict:
    """Chunk items into GEMINI_BATCH_SIZE groups, respecting the daily
    quota (one Gemini call = one unit of quota per chunk). Returns a
    combined dict of id -> analysis for whatever succeeded. Items beyond
    the remaining quota are simply skipped (caller falls back to raw)."""
    if not items:
        return {}
    results = {}
    chunks = list(chunked(items, GEMINI_BATCH_SIZE))
    for chunk in chunks:
        remaining = get_quota_remaining(state)
        if remaining <= 0:
            log.warning("Gemini daily quota exhausted — remaining items will use raw fallback.")
            break
        analyzed = batch_analyze_with_gemini(chunk)
        record_gemini_calls(state, 1)
        results.update(analyzed)
        time.sleep(GEMINI_CALL_DELAY_SECONDS)
    return results


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
            "gemini_quota": {"date": _today_str(), "calls": 0},
        }
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.error(f"state.json corrupted, starting fresh: {e}")
        return {"urls": [], "title_hashes": [], "liquipedia": {}, "last_liquipedia_check": None,
                 "gemini_quota": {"date": _today_str(), "calls": 0}}
    data.setdefault("urls", [])
    data.setdefault("title_hashes", [])
    data.setdefault("liquipedia", {})
    data.setdefault("last_liquipedia_check", None)
    data.setdefault("gemini_quota", {"date": _today_str(), "calls": 0})
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
                  image_url: str = "", importance: str = "") -> bool:
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
    if importance:
        embed["fields"] = [{"name": "الأهمية", "value": importance, "inline": True}]

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
    batch-analyzes eligible items via Gemini before sending sequentially
    (so Discord rate limiting and the send budget stay predictable).
    Returns number of messages sent."""
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

    # --- Pass 1: dedup gate, collect eligible items (no sending yet) ---
    candidates = []  # list of dicts: id, name, link, title, clean_summary, image
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

            if len(candidates) >= sent_budget:
                stats["skip_cap"] += 1
                state["urls"].pop()
                state["title_hashes"].pop()
                seen_urls.discard(link)
                seen_titles.discard(t_hash)
                continue

            candidates.append({
                "id": f"rss-{len(candidates)}",
                "name": name,
                "link": link,
                "title": title,
                "clean_summary": strip_html(summary),
                "image": extract_image(entry),
            })

    # --- Pass 2: batch-analyze all eligible items via Gemini ---
    gemini_items = [
        {"id": c["id"], "kind": "news", "title": c["title"], "context": c["clean_summary"]}
        for c in candidates
    ]
    analysis_map = analyze_items_in_batches(state, gemini_items)

    # --- Pass 3: send sequentially, respecting Discord rate limits ---
    for c in candidates:
        analysis = analysis_map.get(c["id"])
        if analysis:
            stats["gemini_analyzed"] += 1
            send_title = analysis["headline"]
            send_desc = f"**{analysis['subheadline']}**\n\n{analysis['summary']}"
            model_importance = analysis["importance"]
        else:
            stats["gemini_fallback"] += 1
            send_title = c["title"]
            send_desc = f"{NO_ANALYSIS_NOTE}\n\n{c['clean_summary'][:280]}"
            model_importance = "عادي"

        # Safety net: raw title/summary (English) + translated output
        # (Arabic) both get scanned, so keyword/org matches in either
        # language can raise the floor even if the model under-rated it.
        floor_text = f"{c['title']} {c['clean_summary']} {send_title} {send_desc}"
        importance = apply_importance_floor(model_importance, floor_text)

        ok = send_discord(
            title=send_title,
            link=c["link"],
            source=c["name"],
            summary=send_desc,
            image_url=c["image"],
            importance=importance,
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
# Liquipedia phase
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


def is_meaningful_edit(rev: dict, prev_size: int) -> tuple:
    """Structural filter only — no keyword check. Drops bot/minor/tiny edits.
    Returns (keep: bool, reason: str, delta: int) — delta is always the raw
    byte change, useful downstream even when keep=True, to give Gemini a
    concrete signal of how big the edit was."""
    user = (rev.get("user") or "").lower()
    new_size = rev.get("size", 0)
    delta = abs(new_size - prev_size) if prev_size else new_size

    if "bot" in user:
        return False, "bot edit", delta
    if rev.get("minor"):
        return False, "marked minor", delta
    if delta < LIQUIPEDIA_MIN_BYTES_CHANGE:
        return False, f"tiny change ({delta} bytes)", delta
    return True, f"{delta} bytes changed", delta


GAME_NAMES = {
    "counterstrike": "Counter Strike 2", "valorant": "VALORANT",
    "leagueoflegends": "League of Legends", "dota2": "Dota 2",
    "rainbowsix": "Rainbow Six Siege", "rocketleague": "Rocket League",
    "mobilelegends": "Mobile Legends", "honorofkings": "Honor of Kings",
    "pubgmobile": "PUBG Mobile", "fighters": "Fighting Games",
    "easportsfc": "EA Sports FC",
}


def liquipedia_phase(state: dict, first_run: bool, sent_budget: int) -> int:
    """Run Liquipedia collection. Collects meaningful edits, batch-analyzes
    them via Gemini (translation + importance), then sends. Returns number
    of messages sent."""
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

    # --- Pass 1: fetch revisions, filter meaningful edits ---
    candidates = []  # list of dicts: id, page_title, page_url, game, user, comment
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
            keep, reason, delta = is_meaningful_edit(rev, prev_size)
            page_state["size"] = rev.get("size", 0)
            if not keep:
                stats[f"drop_{reason.split()[0]}"] += 1
                continue

            if len(candidates) >= sent_budget:
                stats["skip_cap"] += 1
                page_state["revids"].pop()
                continue

            game = GAME_NAMES.get(rev["wiki"], rev["wiki"])
            comment = (rev.get("comment") or "").strip()[:200] or "بدون ملاحظة"
            user = rev.get("user") or "?"

            candidates.append({
                "id": f"lp-{len(candidates)}",
                "page_title": rev["page_title"],
                "page_url": rev["page_url"],
                "game": game,
                "user": user,
                "comment": comment,
                "delta": delta,
            })

    # --- Pass 2: batch-analyze all eligible edits via Gemini ---
    gemini_items = [
        {
            "id": c["id"],
            "kind": "wiki_edit",
            "title": c["page_title"],
            "context": (
                f"اللعبة: {c['game']} | المحرر: {c['user']} | "
                f"حجم التغيير: {c['delta']} بايت | تعليق التعديل: {c['comment']}"
            ),
        }
        for c in candidates
    ]
    analysis_map = analyze_items_in_batches(state, gemini_items)

    # --- Pass 3: send sequentially ---
    for c in candidates:
        analysis = analysis_map.get(c["id"])
        if analysis:
            stats["gemini_analyzed"] += 1
            send_title = analysis["headline"]
            send_desc = f"**{analysis['subheadline']}**\n\n{analysis['summary']}"
            model_importance = analysis["importance"]
        else:
            stats["gemini_fallback"] += 1
            send_title = c["page_title"]
            send_desc = f"{NO_ANALYSIS_NOTE}\n\n{c['comment']}"
            model_importance = "عادي"

        floor_text = f"{c['page_title']} {c['comment']} {send_title} {send_desc}"
        importance = apply_importance_floor(model_importance, floor_text)

        ok = send_discord(
            title=send_title,
            link=c["page_url"],
            source=f"Liquipedia · {c['game']} · المحرر: {c['user']}",
            summary=send_desc,
            importance=importance,
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
    has passed since the last Liquipedia check."""
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
        log.warning("Missing GEMINI_API_KEY — Gemini analysis disabled, will fall back to raw RSS/Liquipedia content.")

    state = load_state()
    first_run = (
        len(state["urls"]) == 0
        and len(state["title_hashes"]) == 0
        and len(state["liquipedia"]) == 0
    )
    if first_run:
        log.info("FIRST RUN: indexing baseline, no messages will be sent this pass.")

    log.info(f"Gemini quota remaining today: {get_quota_remaining(state)}/{GEMINI_DAILY_QUOTA} (model={GEMINI_MODEL})")

    rss_sent = rss_phase(state, first_run, MAX_MESSAGES_PER_RUN)
    remaining_budget = MAX_MESSAGES_PER_RUN - rss_sent

    lp_sent = 0
    if should_run_liquipedia(state):
        lp_sent = liquipedia_phase(state, first_run, remaining_budget)
        state["last_liquipedia_check"] = datetime.now(timezone.utc).isoformat()
    else:
        log.info(f"Liquipedia phase skipped (last check within {LIQUIPEDIA_MIN_INTERVAL_MINUTES} min)")

    save_state(state)
    git_commit_push("single pass + AI batching")

    log.info(f"=== Pass done. RSS sent: {rss_sent}, Liquipedia sent: {lp_sent}, "
              f"Gemini quota used today: {state['gemini_quota']['calls']}/{GEMINI_DAILY_QUOTA} ===")


if __name__ == "__main__":
    main()
