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
(cron) في run.yml، مو من حلقة داخلية.

=== FEATURE UPDATE (2026-08-11) — روم واحد، تصنيف شامل، تحقق مزدوج ===
بناءً على طلب حازم بتطوير شامل للبوت. التغييرات الرئيسية:

1. تصنيف كامل بكل رسالة: المنطقة (محلي/مينا/عالمي) + اللعبة + الأهمية
   (عاجل/مهم/عادي) + نوع الخبر (مؤكد/تسريب/شائعة)، كلها تطلع من Gemini
   نفسه بنفس الاستدعاء (ما فيه استدعاء إضافي)، مع تلميح مبدئي من نوع
   المصدر بـ feeds.py (region / source_type) يقدر Gemini يتجاوزه لو
   مضمون الخبر يقول عكسه.
2. لون إطار الرسالة يتغير حسب الأهمية (أحمر عاجل، برتقالي مهم، بنفسجي
   عادي، افتراضي لو التحليل فشل).
3. تحقق من مصدر ثاني: كل الأخبار الجديدة بنفس الدورة تتجمع أول شي قبل
   الإرسال، وتنقارن عناوينها ببعض (تشابه كلمات دالة، بدون أي استدعاء
   شبكة إضافي) عشان نعرف لو نفس الخبر مذكور بأكثر من مصدر. النتيجة
   توسم بالرسالة نفسها ("مؤكد من أكثر من مصدر" أو "مصدر واحد فقط")،
   ما توقف ولا تأخر الإرسال أبداً — البوت ما يفلتر ولا يحجب أي خبر.
4. ترتيب الإرسال حسب أولوية المصدر (high/normal/low من feeds.py) صار
   فعلياً مطبق هنا لأول مرة — كان موثق بس مو منفذ فعلياً بالكود القديم.
5. صفحات "Portal:Rumours" (أو "Rumours" لـ Call of Duty) المضافة
   لـ watchlist.py تتعامل كتسريبات تلقائياً، برسالة توضح إنها من قسم
   التسريبات المجتمعي في Liquipedia (اللي أصلاً يصنف كل تسريب بمستوى
   ثقة: Unlikely/Possible/Likely/Certain) وتحوّل القارئ للصفحة نفسها
   للتفاصيل، بدل ما Gemini يحاول يعيد صياغة تسريب ما شافه كامل.
6. تنبيه صحة النظام: لو أكثر من نص مصادر RSS فشلت بدورة وحدة، أو
   Gemini فشل بكل محاولاته، أو مفتاح Gemini مفقود، يترسل تنبيه واضح
   بنفس الروم (بلون مختلف تماماً)، بحد أقصى تنبيه كل 6 ساعات عشان ما
   يصير فيضان تنبيهات لو المشكلة استمرت.

Pipeline (once per invocation):
1. RSS phase: fetch all feeds in feeds.py IN PARALLEL, filter freshness +
   dedup, correlate across sources, analyze via Gemini (with region/type
   hints), sort by priority, send within budget.
2. Liquipedia phase (only if LIQUIPEDIA_MIN_INTERVAL_MINUTES have passed
   since last Liquipedia check): poll watchlist pages, filter
   bot/minor/tiny edits, classify (rumour page vs team page, Arab org
   or not), send.

State is unified in state.json with five collections:
- urls: seen RSS URLs (ring of last 8000)
- title_hashes: normalized title hashes (ring of last 8000)
- liquipedia: per-page seen revids + last seen size
- last_liquipedia_check: ISO timestamp of last Liquipedia phase run
- last_health_alert: ISO timestamp of last system-health alert sent

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
LIQUIPEDIA_MIN_INTERVAL_MINUTES = 10

# RSS parallel fetch settings
RSS_FETCH_WORKERS = 40
RSS_FETCH_TIMEOUT_SECONDS = 10

# Liquipedia API
LIQUIPEDIA_USER_AGENT = "GGNewsAR Bot/2.0 (https://ggnewsar.com; hazem@ggnewsar.com)"
LIQUIPEDIA_RATE_LIMIT_SEC = 2.5
LIQUIPEDIA_BATCH_SIZE = 50
LIQUIPEDIA_MIN_BYTES_CHANGE = 100  # ignore edits smaller than this

# Discord embed color — default/fallback (used when analysis fails or
# importance is "عادي")
EMBED_COLOR = 0x7C3AED
# Importance -> embed color. Red for breaking, amber for important,
# the original purple stays the "normal" baseline so old behavior is
# still the visual default when nothing stands out.
IMPORTANCE_COLORS = {
    "عاجل": 0xDC2626,
    "مهم": 0xF59E0B,
    "عادي": 0x7C3AED,
}
# Distinct, deliberately dull color for internal system-health alerts so
# they never look like a "high importance" news item.
EMBED_COLOR_ALERT = 0x374151

DESC_MAX = 700

# Health-alert cooldown so a persistent problem doesn't flood the room
# with a repeat alert every single 15-minute pass.
ALERT_COOLDOWN_HOURS = 6

# Priority order used to sort candidates before applying the per-run send
# cap. "high" sources (official accounts, tournament organizers, etc.)
# get first claim on the budget if MAX_MESSAGES_PER_RUN is ever hit.
PRIORITY_ORDER = {"high": 0, "normal": 1, "low": 2}

# feeds.py "region" hint -> the Arabic label Gemini is told to expect.
# Anything not in this map (i.e. no region set, meaning a global source)
# defaults to "عالمي".
REGION_HINT_MAP = {"jordan": "محلي", "mena": "مينا"}

# Strip "Source - Article Title" patterns from RSS titles for dedup
SOURCE_SUFFIX_RE = re.compile(r"\s*[\-\|\u2013\u2014:]\s*[^\-\|\u2013\u2014:]{1,40}$")

# ------------------------------------------------------------
# Cross-source correlation (lightweight, no extra network calls)
# ------------------------------------------------------------
# Used only to decide whether to tag a message "مؤكد من أكثر من مصدر" or
# "مصدر واحد فقط" — never to block or delay sending. Purely a soft
# informational signal, so a heuristic word-overlap check is good enough;
# false positives/negatives here don't affect whether news gets through.
STOPWORDS = {
    "the", "a", "an", "of", "in", "on", "to", "for", "and", "is", "are",
    "was", "were", "with", "at", "by", "from", "as", "it", "its", "this",
    "that", "new", "after", "before", "vs", "esports", "team", "teams",
    "game", "games", "announces", "announcement", "news", "official",
    "update", "updates", "report", "reports", "reportedly", "amp",
}


def significant_words(normalized_title: str) -> set:
    """Content words only (len > 2, not a stopword) — used to compare
    titles from different sources for the same underlying story."""
    return {w for w in normalized_title.split() if len(w) > 2 and w not in STOPWORDS}


def titles_match(words_a: set, words_b: set) -> bool:
    """True if two title word-sets look like the same story. Requires
    both a minimum shared-word count and a minimum overlap coefficient
    (shared / smaller set size) so a single shared game name doesn't
    count as a match, but two differently-worded headlines about the
    same transfer/result still match even when one is much longer than
    the other (overlap coefficient handles size-imbalanced sets better
    than Jaccard does for this)."""
    if not words_a or not words_b:
        return False
    shared = words_a & words_b
    if len(shared) < 2:
        return False
    overlap = len(shared) / min(len(words_a), len(words_b))
    return overlap >= 0.5


# ------------------------------------------------------------
# Gemini (direct via Google AI Studio) — news analysis + classification
# ------------------------------------------------------------
GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
GEMINI_TIMEOUT_SECONDS = 20
GEMINI_MAX_RETRIES = 2
GEMINI_MAX_TOKENS = 900

GEMINI_SYSTEM_PROMPT = """أنت محرر أخبار إسبورت لمنصة GGNewsAR، تكتب بالعربية الفصحى البيضاء (لغة يومية مثقفة، مو لغة أدبية أو مترجمة حرفياً).

مهمتك: تحليل خبر إسبورت وإخراج ثمانية عناصر: عنوان رئيسي، عنوان فرعي، ملخص قصير، اسم اللعبة، تصنيف المنطقة، تصنيف الأهمية، نوع الخبر، ومستوى موثوقية (لو تسريب أو شائعة).

قواعد صارمة للعنوان والملخص:
- العنوان الرئيسي: لازم يحتوي اسم اللعبة، يبدأ بأهم معلومة (رقم/إنجاز/حدث)، وينتهي بعلامة استفهام أو تعجب حسب نوع الخبر. لو الخبر عن شراكة أو اتفاق أو تحالف، افتح بكلمة صادمة زي "شراكة!" أو "اتفاق رسمي!" أو "تحالف ضخم!". ممنوع استخدام أي شرطة أو علامة "-" بين الكلمات، بالعنوان أو الملخص أو أي حقل.
- العنوان الفرعي: جملة واحدة قصيرة تضيف تفصيل أو سياق إضافي لم يُذكر بالعنوان الرئيسي، مش تكرار له.
- الملخص: جملتين أو ثلاث قصيرة ومتتالية، تبدأ بفعل مباشر (تأهل، حسم، أنهى، خطف)، أرقام وأسماء بالمقدمة، بدون نقاط أو عناوين فرعية.
- ممنوع أي عبارات حشو أو توحي بالذكاء الاصطناعي مثل: "يأتي ذلك في إطار"، "في خطوة لافتة"، "يُعد علامة فارقة"، "تجدر الإشارة إلى"، "من الجدير بالذكر"، "وفي سياق متصل"، "يُشكل نقلة نوعية"، "شهد عالم الإسبورت"، وصفات فارغة مثل "كبيرة" أو "بارزة" بدون وزن فعلي.
- أسماء اللاعبين: اللقب فقط (Nickname)، بدون الاسم الحقيقي الكامل.
- الأرقام المالية: أرقام كاملة مع فواصل الآلاف (مثال: 1,000,000)، ما تكتبها بالحروف. المبالغ فوق المليون تكتب رقم + "مليون". أي مبلغ بعملة غير الدولار يُحوّل لدولار أمريكي بصيغة "نحو" (مثال: نحو 500,000 دولار).
- لو فيه أكثر من فريق عربي بنفس الخبر، لا تبرز فريق واحد بالعنوان دون مبرر واضح من الخبر نفسه.
- لو المصدر ما فيه معلومات كافية لتأكيد تفصيل معين، لا تختلقه.
- اللعبة اسمها مؤنث نحوياً بالجملة (مثال: "فالورانت حسمت")، واسم الفريق مذكر نحوياً (مثال: "فالكونز حسم").
- استخدم "ألعاب الموبايل" فقط، ما تستخدم "الألعاب الجوّالة" ولا "الألعاب المحمولة".
- لغة محايدة ومحترمة دائماً تجاه كل الفرق واللاعبين، ممنوع كلمات مثل "سحق"، "اكتسح"، "دمّر"، "أذلّ"، "فضح". استخدم بدلها "حسم"، "تفوّق على"، "تغلّب على"، "تخطّى"، "ودّع المنافسة".

قواعد تصنيف الحقول الإضافية:
- game: اسم اللعبة بالإنجليزية بالضبط زي ما يُكتب في Liquipedia (مثال: "Counter-Strike 2"، "VALORANT"، "Dota 2"، "League of Legends"، "Mobile Legends: Bang Bang"، "PUBG Mobile"، "Rainbow Six Siege"، "Rocket League"، "Overwatch"، "Honor of Kings"، "Call of Duty"، "Apex Legends"، "Free Fire"، "EA Sports FC"، "Street Fighter 6"، "Tekken 8"، "Teamfight Tactics"). لو الخبر عن الصناعة العامة بدون لعبة محددة، اكتب "عام".
- region: "محلي" فقط لو الخبر عن الأردن تحديداً (فريق أردني، لاعب أردني، الاتحاد الأردني للرياضات الإلكترونية). "مينا" لو عن أي دولة عربية ثانية أو منظمة/فريق عربي (السعودية، الإمارات، مصر، الكويت، العراق، المغرب، إلخ). "عالمي" لغير كذا. عندك تلميح مبدئي بالأسفل من نوع المصدر، اعتمد عليه إلا إذا مضمون الخبر نفسه يقول عكسه بوضوح.
- importance: "عاجل" لنتيجة نهائي بطولة كبرى، فوز فريق عربي ببطولة أو تأهله لمرحلة حاسمة، انتقال نجم عالمي معروف، قرار صناعي ضخم (استحواذ كبير، عقوبة كبرى، إغلاق منظمة). "مهم" لنتائج أدوار متقدمة، انتقالات لاعبين، شراكات ورعايات، تحديثات لعبة كبيرة. "عادي" لغير كذا (تحليلات، أخبار روتينية، تحديثات صغيرة).
- news_type: "مؤكد" هو الافتراضي. اجعله "تسريب" لو الخبر نفسه يصف انتقال أو قرار لسا ما انأعلن رسمياً (كلمات مثل rumored, reportedly, sources say, expected to, in talks). اجعله "شائعة" لو الخبر ضعيف الاستناد أو مصدره غير محدد بوضوح. عندك تلميح مبدئي بالأسفل من نوع المصدر، لكن لو المصدر معروف بالتسريبات ومحتوى الخبر نفسه إعلان رسمي مؤكد، رجّعه "مؤكد" بغض النظر عن التلميح.
- reliability: فقط لو news_type مو "مؤكد"، اكتب جملة قصيرة توضح قوة الإسناد (مثال: "مصدر واحد غير مؤكد"، "تأكيد من مصدرين مستقلين"، "تسريب من داخل الفريق"). لو news_type يساوي "مؤكد"، خله نص فاضي "".

رد بصيغة JSON فقط، بدون أي نص أو شرح إضافي قبله أو بعده، بالشكل التالي بالضبط:
{"headline": "...", "subheadline": "...", "summary": "...", "game": "...", "region": "...", "importance": "...", "news_type": "...", "reliability": "..."}"""

VALID_REGIONS = ("محلي", "مينا", "عالمي")
VALID_IMPORTANCE = ("عاجل", "مهم", "عادي")
VALID_NEWS_TYPES = ("مؤكد", "تسريب", "شائعة")


def analyze_with_gemini(
    title: str,
    summary: str,
    link: str,
    region_hint: str = "عالمي",
    source_type_hint: str = "مؤكد",
) -> dict | None:
    """
    Analyze one news item via Gemini (Google AI Studio free tier, direct API).
    region_hint / source_type_hint come from the source's metadata in
    feeds.py and are passed in as a starting assumption only — Gemini is
    instructed to override them based on the actual content.
    Returns a dict with headline/subheadline/summary/game/region/
    importance/news_type/reliability, or None on failure.
    """
    if not GEMINI_API_KEY:
        return None

    user_content = (
        f"العنوان الأصلي: {title}\n\n"
        f"محتوى/ملخص الخبر: {summary or 'غير متوفر'}\n\n"
        f"رابط المصدر: {link}\n\n"
        f"تلميح المصدر (مبدئي فقط، اعتمد على مضمون الخبر لو يختلف): "
        f"المنطقة المتوقعة = {region_hint}، نوع المصدر المتوقع = {source_type_hint}"
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

            required = ("headline", "subheadline", "summary", "game", "region", "importance", "news_type")
            if not all(k in parsed and parsed[k] for k in required):
                log.warning(f"Gemini response missing/empty keys: {content[:200]}")
                return None

            parsed.setdefault("reliability", "")
            if parsed["region"] not in VALID_REGIONS:
                parsed["region"] = region_hint
            if parsed["importance"] not in VALID_IMPORTANCE:
                parsed["importance"] = "عادي"
            if parsed["news_type"] not in VALID_NEWS_TYPES:
                parsed["news_type"] = source_type_hint
            return parsed

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
# Classification line — shared by RSS phase and Liquipedia phase
# ============================================================
def build_classification_line(
    region: str,
    game: str,
    importance: str,
    news_type: str,
    reliability: str = "",
    multi_source: bool | None = None,
) -> str:
    """One-line tag block shown at the top of every message's description:
    region · game · importance [· type if not confirmed], plus an
    optional second line for source reliability / multi-source status.
    Never affects whether a message is sent — purely informational."""
    parts = [region, game, importance]
    if news_type != "مؤكد":
        parts.append(news_type)
    line = "**" + " · ".join(parts) + "**"

    extra = []
    if news_type != "مؤكد" and reliability:
        extra.append(f"موثوقية المصدر: {reliability}")
    if multi_source is True:
        extra.append("مؤكد من أكثر من مصدر")
    elif multi_source is False:
        extra.append("مصدر واحد فقط، غير مؤكد بعد من مصدر ثاني")
    if extra:
        line += "\n" + " · ".join(extra)
    return line


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
            "last_health_alert": None,  # ISO timestamp string or None
        }
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.error(f"state.json corrupted, starting fresh: {e}")
        return {"urls": [], "title_hashes": [], "liquipedia": {}, "last_liquipedia_check": None, "last_health_alert": None}
    data.setdefault("urls", [])
    data.setdefault("title_hashes", [])
    data.setdefault("liquipedia", {})
    data.setdefault("last_liquipedia_check", None)
    data.setdefault("last_health_alert", None)
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


def send_discord(
    title: str,
    link: str = "",
    source: str = "",
    summary: str = "",
    image_url: str = "",
    color: int = EMBED_COLOR,
) -> bool:
    """Send one news item to Discord as an embed. Returns True on success."""
    if not DISCORD_WEBHOOK_URL:
        log.error("Discord webhook missing")
        return False

    embed = {
        "title": _clip(title, 256),
        "color": color,
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


def send_system_alert(state: dict, reason: str) -> None:
    """Post a distinctly-colored, clearly-labeled system alert to the
    same room. Rate-limited via state['last_health_alert'] so a
    persistent problem doesn't flood the room every 15 minutes."""
    last = state.get("last_health_alert")
    if last:
        try:
            last_dt = datetime.fromisoformat(last)
            if (datetime.now(timezone.utc) - last_dt) < timedelta(hours=ALERT_COOLDOWN_HOURS):
                log.info(f"Health alert suppressed (cooldown active): {reason}")
                return
        except ValueError:
            pass

    ok = send_discord(
        title="تنبيه النظام: البوت محتاج مراجعة",
        summary=reason,
        source="GGNewsAR Bot · صحة النظام",
        color=EMBED_COLOR_ALERT,
    )
    if ok:
        state["last_health_alert"] = datetime.now(timezone.utc).isoformat()
        log.warning(f"Health alert sent: {reason}")
    else:
        log.error(f"Health alert FAILED TO SEND: {reason}")


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


def rss_phase(state: dict, first_run: bool, sent_budget: int) -> tuple[int, dict]:
    """Run RSS collection. Fetches all sources in parallel, gates for
    freshness/dedup, correlates matching stories across different
    sources, sorts by source priority, then analyzes + sends within the
    budget. Returns (messages_sent, stats_dict)."""
    seen_urls = set(state["urls"])
    seen_titles = set(state["title_hashes"])
    stats = defaultdict(int)
    failed = []
    sent = 0

    feed_lookup = {fi["name"]: fi for fi in RSS_FEEDS}

    log.info(f"RSS phase: {len(RSS_FEEDS)} sources, {RSS_FETCH_WORKERS} parallel workers, freshness={MAX_AGE_HOURS}h")

    fetch_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=RSS_FETCH_WORKERS) as executor:
        futures = {executor.submit(fetch_one_feed, fi): fi for fi in RSS_FEEDS}
        for future in concurrent.futures.as_completed(futures):
            fetch_results.append(future.result())

    # ---- Phase A: gate every entry (freshness/dedup), collect survivors ----
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
                "entry": entry,
                "title": title,
                "link": link,
                "summary": summary,
                "t_hash": t_hash,
                "sig_words": significant_words(normalize_title(title)),
                "feed_info": feed_lookup.get(name, {}),
                "multi_source": False,
            })

    # ---- Phase B: cross-source correlation (no network calls) ----
    for i in range(len(candidates)):
        if candidates[i]["multi_source"]:
            continue
        for j in range(i + 1, len(candidates)):
            if candidates[i]["name"] == candidates[j]["name"]:
                continue
            if titles_match(candidates[i]["sig_words"], candidates[j]["sig_words"]):
                candidates[i]["multi_source"] = True
                candidates[j]["multi_source"] = True

    # ---- Phase C: sort by source priority, then analyze + send within budget ----
    candidates.sort(key=lambda c: PRIORITY_ORDER.get(c["feed_info"].get("priority", "normal"), 1))

    for c in candidates:
        if sent >= sent_budget:
            stats["skip_cap"] += 1
            try:
                state["urls"].remove(c["link"])
            except ValueError:
                pass
            try:
                state["title_hashes"].remove(c["t_hash"])
            except ValueError:
                pass
            seen_urls.discard(c["link"])
            seen_titles.discard(c["t_hash"])
            continue

        clean_summary = strip_html(c["summary"])
        region_hint = REGION_HINT_MAP.get(c["feed_info"].get("region"), "عالمي")
        type_hint = "تسريب" if c["feed_info"].get("source_type") == "leak" else "مؤكد"

        analysis = analyze_with_gemini(c["title"], clean_summary, c["link"], region_hint, type_hint)

        if analysis:
            stats["gemini_analyzed"] += 1
            classification = build_classification_line(
                analysis["region"], analysis["game"], analysis["importance"],
                analysis["news_type"], analysis.get("reliability", ""), c["multi_source"],
            )
            send_title = analysis["headline"]
            send_desc = f"{classification}\n\n**{analysis['subheadline']}**\n\n{analysis['summary']}"
            color = IMPORTANCE_COLORS.get(analysis["importance"], EMBED_COLOR)
        else:
            stats["gemini_fallback"] += 1
            send_title = c["title"]
            send_desc = clean_summary[:280]
            color = EMBED_COLOR

        ok = send_discord(
            title=send_title,
            link=c["link"],
            source=c["name"],
            summary=send_desc,
            image_url=extract_image(c["entry"]),
            color=color,
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

    return sent, stats


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
    "counterstrike": "Counter-Strike 2", "valorant": "VALORANT",
    "leagueoflegends": "League of Legends", "dota2": "Dota 2",
    "rainbowsix": "Rainbow Six Siege", "rocketleague": "Rocket League",
    "mobilelegends": "Mobile Legends", "honorofkings": "Honor of Kings",
    "pubgmobile": "PUBG Mobile", "fighters": "Fighting Games",
    "easportsfc": "EA Sports FC",
    # Added 2026-08-11 to cover EWC 2026 titles that had zero
    # Liquipedia tracking before.
    "overwatch": "Overwatch", "tft": "Teamfight Tactics",
    "callofduty": "Call of Duty", "apexlegends": "Apex Legends",
    "freefire": "Free Fire",
}

# Team-page titles (as they appear on Liquipedia) recognized as Arab
# organizations, used to bump region -> "مينا" and importance -> "عاجل"
# for their page edits. Matched as a substring of the page title so
# both "Team_Falcons" and any sub-page still match.
ARAB_ORG_TITLES = (
    "Team_Falcons", "Falcons", "Twisted_Minds", "Nigma_Galaxy", "Nigma",
    "Geekay_Esports", "Geekay", "FATE_Esports", "FATE", "Team_Vision",
)

# Page titles that represent a wiki's community-tracked rumor/transfer
# mill rather than an official team/tournament page. Added to
# watchlist.py for every wiki on 2026-08-11.
RUMOUR_PAGE_TITLES = ("Portal:Rumours", "Rumours")


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

            is_rumour_page = rev["page_title"] in RUMOUR_PAGE_TITLES
            is_arab = any(tok in rev["page_title"] for tok in ARAB_ORG_TITLES)

            news_type = "تسريب" if is_rumour_page else "مؤكد"
            region = "مينا" if is_arab else "عالمي"
            importance = "عاجل" if (is_arab and not is_rumour_page) else "مهم"
            reliability = "تصنيف Liquipedia المجتمعي بمستويات ثقة (محتمل/مرجّح/شبه مؤكد)، راجع الرابط" if is_rumour_page else ""

            classification = build_classification_line(
                region, game, importance, news_type, reliability, multi_source=None,
            )

            if is_rumour_page:
                send_title = f"تحديث تسريبات: {game}"
                send_desc = (
                    f"{classification}\n\n"
                    f"تحديث جديد على صفحة تسريبات الانتقالات لـ{game} على Liquipedia. "
                    f"راجع الرابط لتفاصيل التسريب ومستوى الثقة فيه.\n\n"
                    f"ملاحظة المحرر: {comment}"
                )
            else:
                send_title = rev["page_title"].replace("_", " ")
                send_desc = f"{classification}\n\n{comment}"

            color = IMPORTANCE_COLORS.get(importance, EMBED_COLOR)

            ok = send_discord(
                title=send_title,
                link=rev["page_url"],
                source=f"Liquipedia · {game} · المحرر: {user}",
                summary=send_desc,
                color=color,
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
        log.warning("Missing GEMINI_API_KEY — Gemini analysis disabled, will fall back to raw RSS titles/summaries.")

    state = load_state()
    first_run = (
        len(state["urls"]) == 0
        and len(state["title_hashes"]) == 0
        and len(state["liquipedia"]) == 0
    )
    if first_run:
        log.info("FIRST RUN: indexing baseline, no messages will be sent this pass.")

    rss_sent, rss_stats = rss_phase(state, first_run, MAX_MESSAGES_PER_RUN)
    remaining = MAX_MESSAGES_PER_RUN - rss_sent

    lp_sent = 0
    if should_run_liquipedia(state):
        lp_sent = liquipedia_phase(state, first_run, remaining)
        state["last_liquipedia_check"] = datetime.now(timezone.utc).isoformat()
    else:
        log.info(f"Liquipedia phase skipped (last check within {LIQUIPEDIA_MIN_INTERVAL_MINUTES} min)")

    # ---- System health check ----
    if not first_run:
        ok_sources = rss_stats.get("sources_ok", 0)
        failed_sources = rss_stats.get("sources_failed", 0)
        total_sources = ok_sources + failed_sources
        analyzed = rss_stats.get("gemini_analyzed", 0)
        fallback = rss_stats.get("gemini_fallback", 0)

        reasons = []
        if total_sources > 0 and failed_sources / total_sources > 0.5:
            reasons.append(
                f"أكثر من نص مصادر RSS فشلت هذه الدورة ({failed_sources} من {total_sources})، "
                f"ممكن يكون فيه مشكلة اتصال عامة."
            )
        if not GEMINI_API_KEY:
            reasons.append("متغير GEMINI_API_KEY غير موجود، البوت يرسل كل الأخبار بدون تحليل أو تصنيف.")
        elif (analyzed + fallback) > 0 and analyzed == 0:
            reasons.append(
                "كل محاولات تحليل Gemini فشلت هذه الدورة، البوت يرسل النصوص الخام بدون صياغة أو تصنيف. "
                "تأكد من صلاحية GEMINI_API_KEY أو حصة الاستخدام اليومية."
            )
        if reasons:
            send_system_alert(state, " | ".join(reasons))

    save_state(state)
    git_commit_push("single pass")
    log.info(f"=== Pass done. RSS sent: {rss_sent}, Liquipedia sent: {lp_sent} ===")


if __name__ == "__main__":
    main()
