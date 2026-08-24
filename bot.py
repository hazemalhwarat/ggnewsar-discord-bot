"""
GGNewsAR Discord Bot — RSS-only pipeline (single-pass edition).

كل خبر RSS يمر أولاً على Gemini (مباشرة عبر Google AI Studio) اللي يحلله
ويطلع عنوان رئيسي وعنوان فرعي وملخص قصير بالفصحى البيضاء حسب ستايل
GGNewsAR، بدل إرسال عنوان/ملخص RSS الخام. لو التحليل فشل، يرجع البوت
تلقائياً للنص الأصلي. الصور تترفق تلقائياً مع كل رسالة لو المصدر فيه صورة
(extract_image()، موجودة من الأول وما تغيرت).

=== ARCHITECTURE CHANGE (2026-07-05) ===
رجعنا لنمط single pass: كل استدعاء يفحص كل المصادر مرة وحدة ويطلع.
الاستمرارية تجيها من GitHub Actions schedule (cron) في run.yml، مو من
حلقة داخلية.

=== FEATURE UPDATE (2026-08-11، الجولة الأولى) — روم واحد، تصنيف شامل ===
تصنيف كامل بكل رسالة (المنطقة، اللعبة، الأهمية، نوع الخبر) يطلع من Gemini
بنفس الاستدعاء، لون الإطار يتغير حسب الأهمية، وتحقق من مصدر ثاني.

=== FEATURE UPDATE (2026-08-11، الجولة الثانية) — حذف Liquipedia نهائياً،
    تحقق عبر نافذة 24 ساعة، فاصل 5 ساعات ===
بناءً على طلب حازم، ثلاث تغييرات جوهرية:

1. حذف Liquipedia بالكامل. البوت الآن RSS فقط — ما فيه أي استدعاء لـ
   Liquipedia API ولا أي رسالة مصدرها Liquipedia. watchlist.py ما عاد
   مستورد ولا مستخدم إطلاقاً (احذفه من الريبو، صار ملف ميت).
2. التحقق من مصدر ثاني صار عبر نافذة متجددة مدتها 24 ساعة، محفوظة في
   state.json (state["recent_titles"])، مو بس مقارنة داخل نفس الدورة.
   بما إن البوت صار يشتغل كل 5 ساعات بدل كل 15 دقيقة، خبر ينشره مصدر
   الساعة 9 وينشره مصدر ثاني الساعة 12 لازم ينلقطوا مع بعض حتى لو كل
   وحدة بدورة تشغيل منفصلة — نافذة الـ24 ساعة (تقريباً 4-5 دورات) هي
   اللي تحقق هذا. كل خبر جديد يتقارن مع كل خبر شافه البوت بآخر 24 ساعة
   (من مصدر مختلف)، بنفس منطق تشابه الكلمات الدالة، بدون أي استدعاء
   شبكة إضافي.
3. الفاصل الزمني صار كل 5 ساعات بدل كل 15 دقيقة (لازم تعدل cron بـ
   run.yml يدوياً، راجع الأسفل). بما إن كل دورة صارت تغطي فترة أطول
   بكثير، رفعت MAX_MESSAGES_PER_RUN من 50 لـ150 عشان ما ينحجز خبر
   بسبب سقف قديم مبني على فاصل 15 دقيقة.

=== FEATURE UPDATE (2026-08-11، الجولة الثالثة) — لا تكرار: تعديل
    الرسالة الأصلية بدل نشر نسخة ثانية ===
المطابقة بين المصادر (نفس منطق كلمات دالة + نافذة 24 ساعة أعلاه) كانت
بس تضيف ملاحظة "مؤكد من أكثر من مصدر" على رسالة جديدة منفصلة — يعني
القصة الوحدة كانت تطلع مرتين أو أكتر بصياغات مختلفة من مصادر مختلفة.
هذا كان مصدر التكرار اللي حازم لاحظه.

الحل: state["sent_stories"] صار يخزن، لكل خبر انبعت فعلياً، الـ
message_id تبعه على Discord (عبر ?wait=true بالـ webhook). لما خبر جديد
يتطابق مع قصة سبق إرسالها (بنفس الدورة أو بآخر 24 ساعة)، البوت ما يرسل
رسالة جديدة إطلاقاً — يعمل PATCH على الرسالة الأصلية نفسها (عبر
edit_discord_message) ويضيف سطر "تأكيد إضافي من: [المصدر]" وتحديث
الفوتر بقائمة كل المصادر اللي غطت القصة. لو نفس المصدر رجع غطى نفس
القصة (تحديث بصياغة تانية)، يتجاهل تماماً بدون أي رسالة أو تعديل.
التعديلات (confirmed_edit) ما تُحسب من سقف MAX_MESSAGES_PER_RUN إطلاقاً
— السقف يطبّق فقط على الأخبار الجديدة فعلياً.

نتيجة لحذف Liquipedia، ميزة التسريبات المبنية على صفحات Portal:Rumours
انحذفت معها بالكامل. عوضتها بمصادر تسريبات بديلة في feeds.py: حسابات
مسربين إضافية + قنوات Google News مخصصة لكل لعبة تبحث تحديداً عن كلمات
دالة على شائعة/تسريب (rumor, reportedly, in talks, leaked)، بالإضافة
لتوسعة عامة بمصادر صناعة وتحليلات وتغطية إقليمية إضافية.

=== FEATURE UPDATE (2026-08-11، الجولة الرابعة) — تحقق من صلة الخبر
    بالإسبورت فعلياً (is_esports) ===
سبب التغيير: مصدر "Syria Esports" في feeds.py (بحث Google News بكلمة
Syria+esports بدون تقييد على موقع) رجّع خبر عن حريق بإسبانيا لأن Google
News، لما ما يلقى نتائج كافية مطابقة فعلياً، يعبّي النتائج بأخبار مرتبطة
بشكل ضعيف (هنا مجرد ذكر اسم الدولة). الخبر طلع خام بدون أي صياغة لأن
Gemini فشل يطلّع الحقول المطلوبة لخبر مش إسبورت أصلاً، فرجع البوت
تلقائياً للنص الخام (fallback القديم) وأرسله زي ما هو.

الحل، تغييرين مترابطين:
1. Gemini الآن يرجّع حقل is_esports أولاً قبل أي تحليل تحريري. لو false،
   البوت يتجاهل الخبر تماماً (لا يُرسل، ولا يرجع للنص الخام).
2. feeds.py: أضيف حقل "loose_query": True على كل مصدر هو بحث Google News
   بكلمة مفتاحية عامة بدون "site:" (قائمة الدول بمنطقة مينا، بحث الأردن
   العام، بحث الرعايات/الأعمال). لهذه المصادر تحديداً، لو استدعاء Gemini
   نفسه فشل تقنياً (timeout، JSON غير صالح، إلخ)، البوت يتجاهل الخبر
   بدل الرجوع للنص الخام — لأن هذي بالضبط المصادر اللي بدون فحص، ممكن
   ترجّع محتوى مالوش أي علاقة بالإسبورت. المصادر التانية (RSS مخصص أو
   site: مقيّد) ما تغيرت، fallback النص الخام لسا آمن فيها.

Pipeline (once per invocation):
RSS phase only: fetch all feeds in feeds.py IN PARALLEL, filter freshness
+ dedup, correlate across sources (same-pass AND against the persisted
24h window), analyze via Gemini (with region/type hints), sort by
priority, send within budget.

State is unified in state.json:
- urls: seen RSS URLs (ring of last 8000)
- title_hashes: normalized title hashes (ring of last 8000)
- sent_stories: rolling 24h window of stories actually sent to Discord —
  {words, at, message_id, webhook_url, sources, title, link, color,
  image_url, base_description} — used to PATCH the original message
  (via edit_discord_message) instead of posting a duplicate when another
  source confirms the same story (pruned automatically)
- last_health_alert: ISO timestamp of last system-health alert sent

Configuration source: feeds.py (RSS_FEEDS) only.
Secrets: DISCORD_WEBHOOK_URL, GEMINI_API_KEY in environment.

GitHub Actions workflow (run.yml) should trigger this via:
  on:
    workflow_dispatch:
    schedule:
      - cron: "0 */5 * * *"   # every 5 hours
  (this line needs updating by hand in run.yml — it isn't part of this file)
"""

import os
import re
import html as html_lib
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

from feeds import RSS_FEEDS, SCRAPERS
from relevance import is_hard_excluded, is_game_content_noise

# Sources that can't be fetched as RSS/Atom (feedparser needs XML; some
# sites, like Sheep Esports post-2026-relaunch, don't expose any XML
# route) are marked "fetch_type": "scraper" in feeds.py, which is also
# where SCRAPERS (name -> function) and the scraper functions
# themselves live. fetch_one_feed() below just dispatches into it.

# ============================================================
# Configuration
# ============================================================

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()

STATE_FILE = Path("state.json")

# No cap on messages per run — per Hazem's instruction (2026-08-11):
# send literally everything esports-related that passes the freshness
# and dedup gates below, no artificial ceiling. Kept as a named constant
# (rather than removing the check entirely) so the safety valve is one
# edit away if a future pathological case ever needs it, but set high
# enough that it will never realistically trigger under normal volume.
#
# Real-world ceiling to be aware of: this is NOT actually unlimited in
# practice. run.yml's job timeout is 10 minutes, and Phase C below
# analyzes+sends candidates one at a time (each Gemini call can take up
# to ~20s with retries, plus MESSAGE_DELAY_SECONDS between sends) — so a
# very large burst (e.g. a heavy EWC 2026 news day) could still get cut
# off mid-run by the Actions timeout before everything sends. Worse,
# state.json is only saved/committed at the very end of main(), so a
# timeout-killed run loses that run's dedup progress entirely and the
# same items would be re-fetched (and likely re-sent) next run. If large
# bursts become common, worth revisiting: parallelize the Gemini calls in
# Phase C (like RSS fetching already is), or raise timeout-minutes in
# run.yml, or save_state()/commit incrementally instead of only at the
# end. Not changed here since it wasn't asked for — flagging so a burst
# doesn't look like another "broken bot" surprise later.
MAX_MESSAGES_PER_RUN = 100000

# Discord webhook rate limit safety margin
MESSAGE_DELAY_SECONDS = 1.0

# RSS freshness window: ignore items older than this
MAX_AGE_HOURS = 24

# State ring sizes. Raised from 8000 (2026-08-17): with ~200 feeds now
# configured (see feeds.py), plus the skip_old fix above already cutting
# churn a lot, this is extra headroom so a real fresh/relevant link can't
# get pushed out before it naturally ages past MAX_AGE_HOURS.
SEEN_URLS_RING = 30000
SEEN_TITLES_RING = 30000

# RSS parallel fetch settings
RSS_FETCH_WORKERS = 40
RSS_FETCH_TIMEOUT_SECONDS = 10

# BUG FIX (2026-08-22 — root cause of Hazem's "spam" report, some stories
# posted twice): state.json used to be saved+committed exactly once, at
# the very end of main(), AFTER every single Discord send in Phase C. On
# a heavy news day (many EWC-style bursts), Gemini's free-tier rate limit
# forces long per-item retry backoffs (see analyze_with_gemini), which can
# push a single run's wall-clock time past run.yml's job timeout. GitHub
# Actions then kills the process mid-Phase-C — after several messages had
# already been posted to Discord successfully — but since state.json was
# never saved, none of that progress survives. The next run starts from
# the OLD state, doesn't recognize those links/titles as seen, and
# reprocesses (and re-sends) them: a real duplicate post for exactly the
# items caught mid-flight, which only shows up on high-volume days — i.e.
# "spam for some news", matching what Hazem described.
# Fix: checkpoint (save_state + git_commit_push) every CHECKPOINT_EVERY_
# RESOLVED durably-resolved candidates inside Phase C, instead of only
# once at the end. A resolved candidate is one whose fate is final (sent,
# confirmed-edit, or confirmed not relevant) — see the durable-write
# points in Phase C below. Candidates that are deferred for a retry next
# run (budget cap, loose-source Gemini failure, Discord send failure)
# are deliberately NEVER written to state — same safety net as before,
# just applied consistently now. This bounds a mid-run kill to losing at
# most a handful of not-yet-reached candidates (which simply get a normal
# retry next run, no duplicate), instead of losing an entire run's worth
# of already-sent progress.
CHECKPOINT_EVERY_RESOLVED = 5

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
# with a repeat alert every single pass.
ALERT_COOLDOWN_HOURS = 6

# Cross-source verification now spans a rolling 24h window persisted in
# state.json, not just the current pass — needed because the bot only
# runs every 5 hours, so a second source confirming a story usually shows
# up in a *different* run, not the same one.
CROSS_SOURCE_WINDOW_HOURS = 24
# Defensive cap on how many recent-title entries we keep, on top of the
# 24h age-based pruning, in case of an unexpectedly huge single pass.
RECENT_TITLES_MAX = 4000

# Priority order used to sort candidates before applying the per-run send
# cap. "high" sources (official accounts, tournament organizers, etc.)
# get first claim on the budget if MAX_MESSAGES_PER_RUN is ever hit.
PRIORITY_ORDER = {"high": 0, "normal": 1, "low": 2}

# feeds.py "region" hint -> the Arabic label Gemini is told to expect.
# Anything not in this map (i.e. no region set, meaning a global source)
# defaults to "عالمي".
REGION_HINT_MAP = {"jordan": "محلي", "mena": "مينا"}

# Strip "Article Title - Source" trailing patterns from RSS titles for
# dedup (e.g. "Falcons win IEM Cologne - HLTV.org" -> "Falcons win IEM
# Cologne"). FIX (2026-08-17): the previous version used `\s*` (zero or
# more spaces) around the hyphen/pipe/dash separator, which meant it also
# matched the bare hyphen INSIDE a match score with no surrounding spaces
# ("Legacy 2-0 MIBR to claim last EWC playoff spot" -> everything from
# "-0" onward got deleted, leaving just "legacy 2"). Since esports
# headlines constantly contain scores like this, that one bug silently
# gutted normalize_title()/title_hash()/significant_words() for a huge
# share of real headlines down to 0-1 meaningful words, which in turn
# broke titles_match()'s "same story, different source/run" fallback
# (it requires >= 2 shared words) and let already-sent stories be
# re-sent as if new. Requiring a real space on both sides of the
# hyphen/pipe/dash (scores never have one: "2-0", never "2 - 0") fixes
# this while still stripping genuine "Title - Source"/"Title | Source"
# suffixes. Colon suffixes ("Recap: Falcons Advance") keep the looser
# "optional space before, required space after" rule since that matches
# how colons are actually used in headlines.
SOURCE_SUFFIX_RE = re.compile(
    r"\s+[\-\|\u2013\u2014]\s+[^\-\|\u2013\u2014:]{1,40}$"
    r"|\s*:\s+[^\-\|\u2013\u2014:]{1,40}$"
)

# ------------------------------------------------------------
# Cross-source correlation (lightweight, no extra network calls)
# ------------------------------------------------------------
# Used to decide whether a new RSS entry is actually the same story as
# something already sent to Discord in the last 24h. A match suppresses
# the duplicate send and instead edits the original message (see
# render_confirmed_embed / edit_discord_message, used from rss_phase). A
# false positive here means a real second story silently gets folded
# into an edit instead of getting its own message, so treat the 0.4
# overlap threshold below as a real gate now, not just a soft label.
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
    # 0.4 tuned empirically: two headlines about the same transfer/result
    # often only share 2 real anchor words (a player name + a team name)
    # once stopwords and each outlet's own framing words are stripped out.
    # This is a soft, informational tag only — it never blocks or delays
    # sending — so erring slightly toward catching more real matches is
    # the right trade-off versus a stricter threshold that misses them.
    return overlap >= 0.4


# ------------------------------------------------------------
# Gemini (direct via Google AI Studio) — news analysis + classification
# ------------------------------------------------------------
GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
GEMINI_TIMEOUT_SECONDS = 20
# FIX (2026-08-21): raised from 2. The free tier for gemini-2.5-flash is
# only ~10 requests/minute. On a heavy news day (EWC volume) a single
# 15-minute run can generate far more than 10 new candidates, so a burst
# of 429s is expected and normal, not a real outage. 2 quick retries at a
# flat 2s often weren't enough to survive that burst, causing the
# health-alert condition (analyzed==0 for the whole run) to fire even
# though Gemini itself was fine — it was just temporarily saturated.
# Kept at 3 rather than pushing higher: the real defense against a big
# burst is now the cross-run retry below (an item that still fails here
# gets picked up again next run, ~15 min later, in a fresh rate-limit
# window), not making one single call fight the limit harder — that would
# risk the run itself blowing past the 10-minute GitHub Actions timeout.
GEMINI_MAX_RETRIES = 3
GEMINI_MAX_TOKENS = 900
# Base/cap for exponential backoff on 429 when the API doesn't tell us
# how long to wait (see analyze_with_gemini): 3s, 6s, 12s (capped at
# GEMINI_429_MAX_BACKOFF) across attempts. A real retryDelay from the
# API response is always preferred when present.
GEMINI_429_BASE_BACKOFF = 3
GEMINI_429_MAX_BACKOFF = 15

# DISABLED 2026-08-23 per Hazem's explicit instruction: no AI step in the
# pipeline anymore. analyze_with_gemini() below is kept in the file
# (unused, never called) purely so it's a one-line change to bring back
# if ever wanted later — Phase C now sends every genuinely-new candidate
# directly, with only relevance.is_hard_excluded() (plain keyword
# matching, no API calls) guarding the "loose_query" sources. See the
# Phase C comment further down for the full reasoning.

GEMINI_SYSTEM_PROMPT = """أنت محرر أخبار إسبورت لمنصة GGNewsAR، تكتب بالعربية الفصحى البيضاء (لغة يومية مثقفة، مو لغة أدبية أو مترجمة حرفياً).

مهمتك أولاً: التأكد إن الخبر فعلاً عن الرياضات الإلكترونية (منافسات ألعاب فيديو، فرق، لاعبين، بطولات، رعايات، قرارات صناعة الإسبورت). بعض المصادر هي بحث Google News بكلمة مفتاحية عامة (مثال: اسم دولة + "esports") بدون أي قيد على الموقع، وأحياناً يرجّع بحث زي هذا أخبار مالها أي علاقة بالإسبورت (كارثة طبيعية، سياسة، رياضة تقليدية) لمجرد إنها تحتوي اسم الدولة. لو الخبر مش عن الإسبورت فعلاً رغم إنه طلع من مصدر بحث بكلمة "esports"، رجّع is_esports: false واترك باقي الحقول نصوص فاضية "". لا تحاول "تأويل" خبر غير متعلق بالإسبورت وصياغته كأنه خبر إسبورت.

لو الخبر فعلاً عن الإسبورت، رجّع is_esports: true وكمّل باقي المهمة: إخراج ثمانية عناصر إضافية: عنوان رئيسي، عنوان فرعي، ملخص قصير، اسم اللعبة، تصنيف المنطقة، تصنيف الأهمية، نوع الخبر، ومستوى موثوقية (لو تسريب أو شائعة).

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
{"is_esports": true, "headline": "...", "subheadline": "...", "summary": "...", "game": "...", "region": "...", "importance": "...", "news_type": "...", "reliability": "..."}

لو الخبر مش عن الإسبورت: {"is_esports": false, "headline": "", "subheadline": "", "summary": "", "game": "", "region": "", "importance": "", "news_type": "", "reliability": ""}"""

VALID_REGIONS = ("محلي", "مينا", "عالمي")
VALID_IMPORTANCE = ("عاجل", "مهم", "عادي")
VALID_NEWS_TYPES = ("مؤكد", "تسريب", "شائعة")


def analyze_with_gemini(
    title: str,
    summary: str,
    link: str,
    region_hint: str = "عالمي",
    source_type_hint: str = "مؤكد",
    loose_query: bool = False,
    stats: dict | None = None,
) -> dict | None:
    """
    Analyze one news item via Gemini (Google AI Studio free tier, direct API).
    region_hint / source_type_hint come from the source's metadata in
    feeds.py and are passed in as a starting assumption only — Gemini is
    instructed to override them based on the actual content.
    loose_query=True means this source is a bare Google News keyword
    search with no site: restriction (feeds.py "loose_query" flag) —
    passed through so Gemini applies extra scrutiny to relevance, since
    these are the sources most likely to surface something that only
    matched on a keyword (e.g. a country name) and isn't esports news
    at all.
    Returns a dict with is_esports/headline/subheadline/summary/game/
    region/importance/news_type/reliability, or None on failure.
    """
    if not GEMINI_API_KEY:
        return None

    source_note = (
        "هذا المصدر بحث Google News بكلمة مفتاحية عامة بدون تقييد على موقع معين "
        "(loose_query) — افحص العلاقة بالإسبورت بعناية إضافية قبل القبول."
        if loose_query else
        "هذا المصدر RSS مخصص أو مقيّد بموقع/حساب معروف بالإسبورت."
    )
    user_content = (
        f"العنوان الأصلي: {title}\n\n"
        f"محتوى/ملخص الخبر: {summary or 'غير متوفر'}\n\n"
        f"رابط المصدر: {link}\n\n"
        f"ملاحظة عن المصدر: {source_note}\n\n"
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
                if stats is not None:
                    stats["gemini_429"] = stats.get("gemini_429", 0) + 1
                # FIX (2026-08-21): honor the API's own retryDelay when it
                # gives one (Gemini's 429 body usually includes
                # error.details[].retryDelay, e.g. "23s") instead of
                # always sleeping a flat 2s — a flat sleep that's shorter
                # than the real quota window just burns through all our
                # retries without ever actually waiting long enough.
                # Falls back to exponential backoff (3s/6s/12s, capped at
                # GEMINI_429_MAX_BACKOFF) if no retryDelay is present in
                # the response.
                wait = None
                try:
                    err_body = r.json()
                    for detail in err_body.get("error", {}).get("details", []):
                        delay = detail.get("retryDelay")
                        if delay:
                            wait = float(str(delay).rstrip("s"))
                            break
                except (ValueError, TypeError, AttributeError):
                    pass
                if wait is None:
                    wait = GEMINI_429_BASE_BACKOFF * (2 ** attempt)
                time.sleep(min(wait, GEMINI_429_MAX_BACKOFF))
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

            if "is_esports" not in parsed:
                log.warning(f"Gemini response missing is_esports key: {content[:200]}")
                return None

            is_esports = parsed["is_esports"]
            if isinstance(is_esports, str):
                is_esports = is_esports.strip().lower() == "true"
            parsed["is_esports"] = bool(is_esports)

            if not parsed["is_esports"]:
                # Not esports content — the item matched a keyword search
                # but isn't real esports news (e.g. a wildfire article that
                # matched "Syria esports"). Editorial fields are expected
                # to be empty in this case; nothing else to validate.
                return parsed

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
# Classification line — built from each RSS item's Gemini analysis
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
            # rolling 24h window of stories actually SENT to Discord:
            # [{"words": [...], "at": iso, "message_id": ..., "webhook_url": ...,
            #   "sources": [...], "title": ..., "link": ..., "color": ...,
            #   "image_url": ..., "base_description": ...}, ...]
            # Used to PATCH the original message instead of re-sending a
            # duplicate when another source confirms the same story.
            "sent_stories": [],
            "last_health_alert": None,  # ISO timestamp string or None
        }
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.error(f"state.json corrupted, starting fresh: {e}")
        return {"urls": [], "title_hashes": [], "sent_stories": [], "last_health_alert": None}
    data.setdefault("urls", [])
    data.setdefault("title_hashes", [])
    data.setdefault("sent_stories", [])
    data.setdefault("last_health_alert", None)
    # Old keys ("recent_titles" from the pre-edit-in-place design, and
    # Liquipedia-era keys removed 2026-08-11) are left untouched if
    # present — harmless, just unused going forward.
    return data


def save_state(state: dict) -> None:
    state["urls"] = state["urls"][-SEEN_URLS_RING:]
    state["title_hashes"] = state["title_hashes"][-SEEN_TITLES_RING:]
    state["sent_stories"] = state["sent_stories"][-RECENT_TITLES_MAX:]
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
) -> tuple[bool, str | None]:
    """Send one news item to Discord as an embed. Returns (ok, message_id).
    message_id (via ?wait=true on the webhook call) lets us PATCH this
    exact message later if a second source confirms the same story
    within 24h, instead of posting a duplicate — see edit_discord_message()
    and render_confirmed_embed(). message_id is None if the send failed
    or Discord didn't return one."""
    if not DISCORD_WEBHOOK_URL:
        log.error("Discord webhook missing")
        return False, None

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
    sep = "&" if "?" in DISCORD_WEBHOOK_URL else "?"
    post_url = f"{DISCORD_WEBHOOK_URL}{sep}wait=true"

    for attempt in range(3):
        try:
            r = requests.post(post_url, json=payload, timeout=15)
            if r.status_code in (200, 201):
                try:
                    return True, r.json().get("id")
                except ValueError:
                    return True, None
            if r.status_code == 204:
                return True, None
            if r.status_code == 429:
                retry_after = r.json().get("retry_after", 1)
                time.sleep(float(retry_after) + 0.5)
                continue
            log.error(f"Discord {r.status_code}: {r.text[:200]}")
            return False, None
        except requests.RequestException as e:
            log.error(f"Discord request failed (attempt {attempt + 1}): {e}")
            time.sleep(2)
    return False, None


def edit_discord_message(webhook_url: str, message_id: str, embed: dict) -> bool:
    """PATCH a previously-sent webhook message's embed. Used instead of
    posting a duplicate when a second (or third...) source confirms a
    story that was already sent to Discord within the last 24h."""
    if not webhook_url or not message_id:
        return False
    url = f"{webhook_url.rstrip('/')}/messages/{message_id}"
    payload = {"embeds": [embed]}
    for attempt in range(3):
        try:
            r = requests.patch(url, json=payload, timeout=15)
            if r.status_code in (200, 204):
                return True
            if r.status_code == 429:
                retry_after = r.json().get("retry_after", 1)
                time.sleep(float(retry_after) + 0.5)
                continue
            log.error(f"Discord edit {r.status_code}: {r.text[:200]}")
            return False
        except requests.RequestException as e:
            log.error(f"Discord edit request failed (attempt {attempt + 1}): {e}")
            time.sleep(2)
    return False


def render_confirmed_embed(entry: dict) -> dict:
    """Rebuild the embed for a story that just got confirmed by an
    additional source, from the stored entry in state["sent_stories"].
    Keeps the original title/link/color/image untouched and appends a
    'confirmed by' note plus an updated footer listing every source that
    has now reported the story."""
    sources = entry["sources"]
    desc = entry.get("base_description") or ""
    if len(sources) > 1:
        note = "تأكيد إضافي من: " + "، ".join(sources[1:])
        desc = f"{desc}\n\n{note}" if desc else note
    embed = {
        "title": _clip(entry["title"], 256),
        "color": entry["color"],
    }
    if entry.get("link"):
        embed["url"] = entry["link"]
    if desc:
        embed["description"] = _clip(desc, DESC_MAX)
    embed["footer"] = {"text": _clip(" · ".join(sources), 2048)}
    if entry.get("image_url"):
        embed["image"] = {"url": entry["image_url"]}
    return embed


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

    ok, _ = send_discord(
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


FEED_NAME_NOTE_RE = re.compile(r"\s*\[[^\]]*\]\s*$")


def clean_source_name(name: str) -> str:
    """Strip trailing bracketed notes from a feeds.py entry name before
    using it as the public-facing Discord footer. feeds.py names carry
    internal documentation tags for Hazem's own reference — "[checked,
    EWCF national-team series]", "[unsure]", "[likely]", "[federation]",
    "[thin]" — never meant to be shown to readers. Was previously passed
    through as-is, so these notes were leaking straight into the footer
    of every message from that source."""
    return FEED_NAME_NOTE_RE.sub("", name).strip() or name


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
    """Strip HTML tags AND decode HTML entities (&nbsp;, &#8217;, &amp;,
    etc.). Previously only stripped tags, so raw RSS summaries used as a
    Gemini-fallback (when Gemini analysis fails/is unavailable) showed
    literal entity codes like "Today&#8217;s" or "&nbsp;&nbsp;" instead
    of readable text. This was always latent here, just invisible while
    Gemini analysis was succeeding and rewriting the text anyway."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_lib.unescape(text)
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

    if feed_info.get("fetch_type") == "scraper":
        scraper_fn = SCRAPERS.get(feed_info.get("scraper"))
        if not scraper_fn:
            return name, None, f"no scraper registered for {feed_info.get('scraper')!r}"
        return scraper_fn(feed_info)

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


# ------------------------------------------------------------
# Live/direct match-result filter (added 2026-08-11 per Hazem)
# ------------------------------------------------------------
# Hazem wants literally everything esports-related EXCEPT raw match
# results straight off the sites — both flavors:
#   1) "bare result" articles: a site auto-publishing just the outcome
#      of a single match with no real news value (e.g. "Falcons defeat
#      Vitality 2-0", "Team A beat Team B 16-9").
#   2) live score-ticker content: round-by-round / map-by-map updates
#      published while a match is still in progress ("LIVE", "Map 2:
#      13-11", "Round 14 update").
# This is a heuristic on the title text (cheap, no extra network calls,
# runs in Phase A alongside freshness/dedup). It is intentionally NOT
# applied to Gemini's own output — a headline Gemini writes has already
# been through editorial judgment, so this only filters incoming raw
# RSS titles before they become candidates. Genuine news that happens to
# mention a score in passing (a transfer, a sponsorship, a tournament
# announcement) should not match these patterns; if real news starts
# getting caught, tighten SCORE_RESULT_VERBS_RE / TEAM_VS_SCORE_RE below
# rather than the freshness/dedup gates.
_SCORE_NUM = r"\d{1,2}"
_SCORE_SEP = r"[-:–—]"
SCORE_PATTERN_RE = re.compile(rf"\b{_SCORE_NUM}\s*{_SCORE_SEP}\s*{_SCORE_NUM}\b")

LIVE_TICKER_RE = re.compile(
    r"(?:^live\b|\blive[:\-]|\blive score|\blive updates?\b|\blive thread\b|"
    r"\bmap\s*\d+\b.{0,20}\b(?:score|update)\b|"
    r"\bround\s*\d+\b.{0,20}\b(?:score|update)\b|"
    r"\bgame\s*\d+\b.{0,20}\b(?:score|update)\b)",
    re.IGNORECASE,
)

RESULT_VERBS_RE = re.compile(
    r"\b(?:def\.?|defeats?|defeated|beats?|beaten|tops?|topped|downs?|downed|"
    r"edges?|edged|outlasts?|outlasted|sweeps?|swept|stuns?|stunned|upsets?|"
    r"thrash(?:es|ed)?|routs?|routed|crush(?:es|ed)?|wins?\s+(?:over|against)|"
    r"advances?\s+past|clinch(?:es|ed)?\s+(?:win|victory))\b",
    re.IGNORECASE,
)

# "Team Name 2-0 Team Name" shape — a capitalized word/phrase, a score,
# then another capitalized word/phrase, with nothing else meaningful
# around it. Deliberately narrow (short segments only) to avoid matching
# a real headline that merely contains a number near a proper noun.
TEAM_VS_SCORE_RE = re.compile(
    rf"^[A-Z][\w.'’]*(?:\s+[A-Z][\w.'’]*){{0,3}}\s+{_SCORE_NUM}\s*{_SCORE_SEP}\s*{_SCORE_NUM}"
    rf"\s+[A-Z][\w.'’]*(?:\s+[A-Z][\w.'’]*){{0,3}}$"
)

RESULT_PREFIX_RE = re.compile(
    r"^\s*(?:result|results|final score|ft|recap)\s*:", re.IGNORECASE
)


def looks_like_live_result(title: str) -> bool:
    """True if this title looks like a bare match-result report or a
    live in-match score update, per Hazem's exclusion rule — not a
    perfect classifier, just a fast text heuristic."""
    if LIVE_TICKER_RE.search(title):
        return True
    if RESULT_PREFIX_RE.search(title):
        return True
    if RESULT_VERBS_RE.search(title) and SCORE_PATTERN_RE.search(title):
        return True
    if TEAM_VS_SCORE_RE.match(title.strip()):
        return True
    return False


# ------------------------------------------------------------------
# Contentless "TEAM VS TEAM" fixture stubs — added 2026-08-24.
#
# The official-site Google News bridges (valorantesports.com,
# lolesports.com, ...) publish one page per scheduled match. Google News
# treats each as an article, but the body is empty: the description just
# repeats the matchup title. In Discord these show up as an endless wall
# of "JDG VS TYL - VALORANT Esports / JDG VS TYL VALORANT Esports", with
# zero actual news in them.
#
# looks_like_live_result() above only catches matchups that carry a
# SCORE ("T1 2-0 DFM"). These score-less schedule/VOD stubs have no
# number, so they walked straight through every gate. This helper closes
# that hole WITHOUT risking real stories: it fires only when BOTH
#   (1) the title, after stripping a trailing " - <Game> Esports" tail,
#       is a bare "X vs Y" matchup and nothing else, AND
#   (2) the description adds no real word beyond the title (only the game
#       name / "esports" / feed boilerplate).
# A real match writeup fails (1) (its title says more than "X vs Y") or
# fails (2) (its description carries actual prose), so it is never
# dropped here. Validated against the exact screenshot junk plus a set of
# real-article cases before shipping.
# ------------------------------------------------------------------
_MATCHUP_TAIL_RE = re.compile(
    r"\s*[-–—|:]\s*[\w .'’&]+?\be-?sports?\b\s*$", re.IGNORECASE
)
_MATCHUP_CORE_RE = re.compile(
    r"^[\w.'’&]{1,20}(?:\s+[\w.'’&]{1,20}){0,2}\s+vs\.?\s+"
    r"[\w.'’&]{1,20}(?:\s+[\w.'’&]{1,20}){0,2}$",
    re.IGNORECASE,
)
_STUB_WORD_RE = re.compile(r"[a-z0-9\u0600-\u06ff]+")
_STUB_FILLER_WORDS = {
    "vs", "esports", "esport", "valorant", "cs2", "csgo", "lol", "league",
    "of", "legends", "dota", "dota2", "rocket", "pubg", "mobile",
    "overwatch", "the", "official", "via", "google", "news", "match",
    "vod", "watch", "highlights", "game",
}


def is_contentless_matchup_stub(title: str, summary: str) -> bool:
    """True for a bare 'TEAM VS TEAM' fixture page whose description adds
    no real content beyond the title — a schedule/VOD stub, not news."""
    core = _MATCHUP_TAIL_RE.sub("", (title or "").strip())
    if not _MATCHUP_CORE_RE.match(core):
        return False
    title_words = set(_STUB_WORD_RE.findall((title or "").lower()))
    summ_words = set(_STUB_WORD_RE.findall((summary or "").lower()))
    return len(summ_words - title_words - _STUB_FILLER_WORDS) == 0


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
                # FIX (2026-08-17): don't spend ring capacity on stale
                # items. An item's age only ever increases, so anything
                # that fails freshness now will fail it again on every
                # future run regardless of whether it's "seen" — tracking
                # it here bought nothing but ate ring slots. With ~180
                # feeds (many broad Google News searches returning lots
                # of old/irrelevant entries every run) this was silently
                # cycling ~40% of the entire 8000-slot ring on a single
                # run, which evicted still-fresh, still-relevant links
                # (like a same-day HLTV article) long before they aged
                # out of the real MAX_AGE_HOURS window — making the bot
                # treat a link it already sent hours earlier as brand
                # new again. See also SEEN_URLS_RING/SEEN_TITLES_RING
                # below, raised as extra headroom on top of this fix.
                stats["skip_old"] += 1
                continue
            if t_hash in seen_titles:
                stats["skip_dup_title"] += 1
                seen_urls.add(link); state["urls"].append(link)
                continue

            if looks_like_live_result(title):
                stats["skip_live_result"] += 1
                seen_urls.add(link); state["urls"].append(link)
                seen_titles.add(t_hash); state["title_hashes"].append(t_hash)
                if stats["skip_live_result"] <= 15:
                    log.info(f"  filtered as live/direct result: {title!r} ({name})")
                continue

            if is_contentless_matchup_stub(title, summary):
                stats["skip_matchup_stub"] += 1
                seen_urls.add(link); state["urls"].append(link)
                seen_titles.add(t_hash); state["title_hashes"].append(t_hash)
                if stats["skip_matchup_stub"] <= 15:
                    log.info(f"  filtered as contentless matchup stub: {title!r} ({name})")
                continue

            # Passes all gates so far. Mark seen IN MEMORY ONLY (this
            # run's own seen_urls/seen_titles sets) so two feeds surfacing
            # the same link/title within this same fetch still dedup
            # correctly against each other. Do NOT write to
            # state["urls"]/state["title_hashes"] yet — that durable,
            # on-disk write is deferred to Phase C, at the exact moment
            # this candidate is actually resolved (sent / confirmed-edit /
            # confirmed not relevant). See CHECKPOINT_EVERY_RESOLVED above
            # for why this split matters.
            seen_urls.add(link)
            seen_titles.add(t_hash)

            if first_run:
                # No Phase C on the first run (nothing is ever sent), so
                # there's no later resolution point to defer to — record
                # the durable baseline right here instead.
                stats["baseline_recorded"] += 1
                state["urls"].append(link)
                state["title_hashes"].append(t_hash)
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
            })

    # ---- Phase B: load the active-stories pool (no network calls) ----
    # "active_stories" holds every story already SENT to Discord — either
    # earlier in this very pass, or in an earlier run within the last 24h
    # (state["sent_stories"], persisted). Phase C checks each new
    # candidate against this pool *before* sending anything: a real match
    # means the story was already posted, so instead of posting a
    # duplicate we PATCH the original Discord message to note the
    # additional source. Entries here are the same dict objects stored in
    # state["sent_stories"], so appending a confirming source to one of
    # them below mutates state in place — no separate merge step needed
    # at save time beyond re-pruning by the 24h cutoff.
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=CROSS_SOURCE_WINDOW_HOURS)
    history = []
    for h in state.get("sent_stories", []):
        try:
            h_at = datetime.fromisoformat(h["at"])
        except (KeyError, ValueError):
            continue
        if h_at >= cutoff:
            history.append(h)
    stats["history_window_size"] = len(history)
    # BUG FIX (2026-08-22): prune state["sent_stories"] in place right now
    # (instead of only at the very end of this function) and make
    # active_stories THE SAME list object, not a copy. A brand-new story
    # sent during this pass is appended directly to state["sent_stories"]
    # below (see mark_resolved/new_entry), so a mid-run checkpoint
    # (save_state + git_commit_push) captures it immediately — including
    # its "sources" list, which is what the 24h cross-source-confirmation
    # feature (titles_match / render_confirmed_embed) depends on. Before
    # this fix, newly-sent stories only entered state["sent_stories"] in a
    # single batch at the very end of rss_phase — so a mid-run kill lost
    # them from that list even though the checkpoint fix above already
    # protected the plain dedup rings, meaning a second source confirming
    # the same story next run could still get posted as its own separate
    # message instead of being merged into the original via an edit.
    state["sent_stories"] = history
    active_stories = history

    # ---- Phase C: sort by source priority, then match/edit or analyze + send ----
    candidates.sort(key=lambda c: PRIORITY_ORDER.get(c["feed_info"].get("priority", "normal"), 1))

    resolved_since_checkpoint = 0

    def mark_resolved(c: dict) -> None:
        """Durable write: this candidate's fate is final (sent, confirmed
        by another source, or confirmed not relevant) — safe to persist.
        Also checkpoints (save_state + git_commit_push) every
        CHECKPOINT_EVERY_RESOLVED calls so a mid-run kill can't lose more
        than a handful of items' worth of progress. See the BUG FIX note
        by CHECKPOINT_EVERY_RESOLVED above."""
        nonlocal resolved_since_checkpoint
        state["urls"].append(c["link"])
        state["title_hashes"].append(c["t_hash"])
        resolved_since_checkpoint += 1
        if resolved_since_checkpoint >= CHECKPOINT_EVERY_RESOLVED:
            save_state(state)
            git_commit_push("checkpoint mid-run")
            resolved_since_checkpoint = 0

    # ---- Phase C, step 1: match-check every candidate against
    # active_stories, in order (cheap, no network — this must stay
    # sequential since a send in this same pass can grow active_stories
    # and affect a later candidate's match check). Matches are resolved
    # immediately (edit or skip-dup); anything left over is a genuinely
    # new story and goes into new_candidates for step 2. ----
    new_candidates = []
    for c in candidates:
        match = None
        for st in active_stories:
            if titles_match(c["sig_words"], set(st["words"])):
                match = st
                break

        if match is not None:
            if clean_source_name(c["name"]) in match["sources"]:
                # Same source re-reporting the same story it already
                # posted (reworded headline, follow-up blurb, etc.) —
                # nothing new to say, skip silently.
                stats["skip_dup_story"] += 1
                mark_resolved(c)
                continue
            match["sources"].append(clean_source_name(c["name"]))
            new_embed = render_confirmed_embed(match)
            if edit_discord_message(match["webhook_url"], match["message_id"], new_embed):
                stats["confirmed_edit"] += 1
            else:
                stats["confirmed_edit_failed"] += 1
            # Resolved either way — this candidate is the same underlying
            # story as `match`, so there's nothing to gain by retrying it
            # next run even if the PATCH itself failed (a Discord hiccup,
            # not a reason to treat the story as unseen).
            mark_resolved(c)
            time.sleep(MESSAGE_DELAY_SECONDS)
            continue

        new_candidates.append(c)

    # ---- Phase C, step 2: no AI step anymore (removed 2026-08-23 per
    # Hazem's explicit instruction — Gemini was the whole reason so much
    # esports news never made it through: free-tier rate limits + slow
    # sequential calls were causing real stories to time out and expire
    # unsent, exactly the "sends almost nothing" complaint). Every
    # genuinely-new candidate now sends directly. The ONLY check left is
    # relevance.is_hard_excluded() — a fast, free, keyword-only guard —
    # and ONLY for "loose_query" sources (bare Google News keyword
    # searches with no site: restriction, e.g. "Syria esports"), since
    # those are the one category that can otherwise pull in something
    # totally unrelated that merely matched a country name (a wildfire
    # article matching "Syria esports", a hardware sale matching "gaming
    # deal"). Every other source here (dedicated esports RSS feeds,
    # site:-restricted bridges, named team/org accounts) is already
    # topic-restricted by construction and sends unconditionally — no
    # keyword gate needed or wanted, per "خل البوت يرسل كل شي ايسبورتس".
    for c in new_candidates:
        if sent >= sent_budget:
            # Deliberately NOT calling mark_resolved(): this candidate was
            # never durably written to state (see the deferred-write fix
            # above), so simply not writing it now means next run will see
            # it as unseen and give it a normal retry — no cleanup needed.
            stats["skip_cap"] += 1
            continue

        clean_summary = strip_html(c["summary"])
        is_loose_source = bool(c["feed_info"].get("loose_query"))

        if is_loose_source and is_hard_excluded(f"{c['title']} {clean_summary}"):
            # Clearly not esports (story-game walkthrough, hardware deal,
            # unrelated media, etc.) AND no known esports team/org/
            # tournament name overrides it — see relevance.py. Final,
            # durable verdict, never worth retrying.
            stats["skip_not_esports"] += 1
            mark_resolved(c)
            continue

        # Game-content gate (2026-08-24) — applies to ALL sources, not
        # just loose ones. GGNewsAR is an esports wire: drop items about
        # the game itself (patch/meta/skins/launch/review/guide/cosplay)
        # UNLESS they tie to the pro scene, a tournament, an org, or a
        # person's org move. This is the one filter that intentionally
        # runs on trusted esports RSS feeds too, because those feeds
        # (Dexerto, GameRiv, etc.) mix game-content posts in with real
        # esports news. is_game_content_noise() only fires when a
        # game-content term is present AND no esports/pro/business signal
        # is — so genuine esports stories always pass. See relevance.py.
        if is_game_content_noise(f"{c['title']} {clean_summary}"):
            stats["skip_game_content"] += 1
            mark_resolved(c)
            continue

        stats["sent_raw"] += 1
        send_title = c["title"]
        send_desc = clean_summary[:280]
        color = EMBED_COLOR

        img_url = extract_image(c["entry"])
        ok, message_id = send_discord(
            title=send_title,
            link=c["link"],
            source=clean_source_name(c["name"]),
            summary=send_desc,
            image_url=img_url,
            color=color,
        )
        if ok:
            sent += 1
            stats["sent"] += 1
            new_entry = {
                "words": sorted(c["sig_words"]),
                "at": now.isoformat(),
                "message_id": message_id,
                "webhook_url": DISCORD_WEBHOOK_URL,
                "sources": [clean_source_name(c["name"])],
                "title": send_title,
                "link": c["link"],
                "color": color,
                "image_url": img_url or "",
                "base_description": send_desc,
            }
            # Only usable for future edits if Discord actually gave us a
            # message_id (needs wait=true to have succeeded); still counts
            # as sent either way, it just won't get a confirmation edit
            # later — a same-story duplicate send is still far better
            # than the old always-send behavior.
            active_stories.append(new_entry)
            # active_stories IS state["sent_stories"] (same list object,
            # see the fix above) so this append is already durable the
            # next time mark_resolved()'s checkpoint fires — no separate
            # end-of-function merge step needed anymore.
            mark_resolved(c)
            time.sleep(MESSAGE_DELAY_SECONDS)
        else:
            # BUG FIX (2026-08-22): previously this candidate was already
            # durably marked seen (back when Phase A wrote to state
            # immediately), so a Discord send failure here — 3 internal
            # retries in send_discord() all failing, e.g. a Discord outage
            # — silently and permanently lost the story with zero retry.
            # Deliberately not calling mark_resolved(): next run will see
            # it as unseen and try sending it again.
            stats["send_failures"] += 1

    # ---- Final cap on sent_stories ----
    # state["sent_stories"] IS active_stories (same list object, see the
    # Phase B fix above): it already holds the pruned 24h history plus
    # every story sent this pass, kept up to date incrementally the whole
    # way through Phase C (including surviving a mid-run checkpoint). Only
    # the defensive size cap is still needed here.
    state["sent_stories"] = state["sent_stories"][-RECENT_TITLES_MAX:]

    log.info("--- RSS Summary ---")
    for k in sorted(stats.keys()):
        log.info(f"  {k:30s} {stats[k]}")
    if failed:
        log.info(f"--- Failed Sources ({len(failed)}) ---")
        for line in failed:
            log.info(f"  - {line}")

    return sent, stats


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
    )
    if first_run:
        log.info("FIRST RUN: indexing baseline, no messages will be sent this pass.")

    rss_sent, rss_stats = rss_phase(state, first_run, MAX_MESSAGES_PER_RUN)

    # ---- System health check ----
    if not first_run:
        ok_sources = rss_stats.get("sources_ok", 0)
        failed_sources = rss_stats.get("sources_failed", 0)
        total_sources = ok_sources + failed_sources

        reasons = []
        if total_sources > 0 and failed_sources / total_sources > 0.5:
            reasons.append(
                f"أكثر من نص مصادر RSS فشلت هذه الدورة ({failed_sources} من {total_sources})، "
                f"ممكن يكون فيه مشكلة اتصال عامة."
            )
        if reasons:
            send_system_alert(state, " | ".join(reasons))

    save_state(state)
    git_commit_push("single pass")
    log.info(f"=== Pass done. RSS sent: {rss_sent} ===")


if __name__ == "__main__":
    main()
