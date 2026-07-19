"""
GGNewsAR Bot — Player Birthdays module
=======================================

الهدف
-----
يطلع من صفحات اللاعبين على Liquipedia كل لاعب عربي (حسب تصنيف الجنسية على
كل ويكي)، يسحب تاريخ ميلاده من الـ infobox، ويخزن النتيجة محلياً. بعدها
بشكل يومي يتحقق هل اليوم يصادف عيد ميلاد أي لاعب من القائمة، ولو فيه
يرسل تنبيه Discord.

ليش مرحلتين منفصلتين (refresh / check)
---------------------------------------
سحب كل صفحات اللاعبين من كل تصنيفات الجنسية على كل الويكيات عملية ثقيلة
(مئات الطلبات لـ Liquipedia API). ما نبيها تتكرر كل يوم. فـ:

  refresh -> يعيد بناء القائمة الكاملة من الصفر ويحفظها في
             players_birthdays.json. يفترض يتشغل مرة كل أسبوع تقريباً
             (لاعبين جدد / انتقالات نادراً ما تغيّر التاريخ نفسه على أي حال).

  check   -> عملية خفيفة تقرأ الملف المحفوظ فقط (بدون أي طلب شبكة لـ
             Liquipedia)، تقارن شهر/يوم اليوم بكل لاعب، وترسل تنبيه لأي
             تطابق ما تم إرساله لهذا التاريخ من قبل. يتشغل يومياً.

طريقة الاستخراج (بدون مفتاح API)
---------------------------------
1. لكل ويكي لعبة، لكل دولة عربية: طلب category members لتصنيف
   "Category:<Nationality> Players" (مع أكثر من اسم مرشح للتصنيف، لأن
   التسمية مو موحدة 100% بين كل الويكيات).
2. لكل صفحة لاعب رجعت: جلب الـ wikitext وطلوع تاريخ الميلاد من داخل
   الـ infobox (يدعم أكثر من صيغة: |birth_date=YYYY-MM-DD، أو
   {{birth date|Y|M|D}}، أو {{birth date and age|Y|M|D}}).
3. لو ما لقينا تاريخ ميلاد (كثير لاعبين يخفون سنة الميلاد أو ما يذكرونها
   أصلاً) نتجاهل اللاعب — نادر جداً نلقى شهر/يوم بدون سنة، فما فيه داعي
   لمعالجة خاصة لهالحالة.

ملاحظة مهمة عن أسماء التصنيفات
--------------------------------
أسماء تصنيفات الجنسية تحت لكل قد تختلف شوي عن الاسم الفعلي المستخدم في
ويكي معين (مثلاً بعض الويكيات تستخدم "United Arab Emirates Players" بدل
"Emirati Players"). كل دولة فيها أكثر من اسم مرشح (candidates) يتم تجربتهم
بالترتيب، وأي تصنيف مو موجود يتم تجاوزه بهدوء (log فقط، بدون فشل). لو
لاحظت دولة أو لعبة ناقصة لاعبين معروفين، تأكد من اسم التصنيف الفعلي على
تلك الويكي وضيفه لقائمة الـ candidates.
"""

import json
import logging
import re
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

log = logging.getLogger("ggnewsar-birthdays")
if not log.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

# ============================================================
# Config
# ============================================================

LIQUIPEDIA_USER_AGENT = "GGNewsARBot/1.0 (Arabic esports news; contact: ggnewsar)"
LIQUIPEDIA_RATE_LIMIT_SEC = 1.2  # مجاملة لـ Liquipedia — لا تنزلها

CACHE_FILE = Path("players_birthdays.json")
STATE_FILE = Path("birthdays_state.json")
SENT_RING_SIZE = 3000

AMMAN_TZ = ZoneInfo("Asia/Amman")

# ويكيات الألعاب اللي GGNewsAR يغطيها. المفتاح = مسار الويكي في رابط
# Liquipedia (liquipedia.net/<key>/...)، القيمة = اسم اللعبة يظهر بالتنبيه.
GAME_WIKIS = {
    "counterstrike":   "CS2",
    "valorant":        "VALORANT",
    "dota2":           "Dota 2",
    "leagueoflegends": "League of Legends",
    "rainbowsix":      "Rainbow Six Siege",
    "rocketleague":    "Rocket League",
    "mobilelegends":   "Mobile Legends",
    "pubgmobile":      "PUBG Mobile",
    "honorofkings":    "Honor of Kings",
    "easportsfc":      "EA Sports FC",
    "fighters":        "Fighting Games",
}

# الدولة بالعربي -> أسماء تصنيف مرشحة على Liquipedia (بدون "Category:").
# أول اسم يلقى صفحات فيه هو المستخدم؛ الباقي احتياطي.
NATIONALITY_CATEGORY_CANDIDATES = {
    "السعودية":  ["Saudi Arabian Players", "Saudi Arabia Players"],
    "الإمارات":  ["Emirati Players", "United Arab Emirates Players", "UAE Players"],
    "مصر":       ["Egyptian Players", "Egypt Players"],
    "الأردن":    ["Jordanian Players", "Jordan Players"],
    "الكويت":    ["Kuwaiti Players", "Kuwait Players"],
    "قطر":       ["Qatari Players", "Qatar Players"],
    "البحرين":   ["Bahraini Players", "Bahrain Players"],
    "عُمان":     ["Omani Players", "Oman Players"],
    "لبنان":     ["Lebanese Players", "Lebanon Players"],
    "العراق":    ["Iraqi Players", "Iraq Players"],
    "سوريا":     ["Syrian Players", "Syria Players"],
    "فلسطين":    ["Palestinian Players", "Palestine Players"],
    "المغرب":    ["Moroccan Players", "Morocco Players"],
    "الجزائر":   ["Algerian Players", "Algeria Players"],
    "تونس":      ["Tunisian Players", "Tunisia Players"],
    "ليبيا":     ["Libyan Players", "Libya Players"],
    "السودان":   ["Sudanese Players", "Sudan Players"],
    "اليمن":     ["Yemeni Players", "Yemen Players"],
}

BIRTHDAY_EMBED_COLOR = 0xEC4899  # وردي — يميّز تنبيهات الميلاد عن الأخبار


# ============================================================
# Liquipedia fetch helpers
# ============================================================

def _lp_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": LIQUIPEDIA_USER_AGENT,
        "Accept-Encoding": "gzip",
    })
    return s


def _lp_get(session: requests.Session, wiki: str, params: dict) -> dict:
    """طلب واحد لـ Liquipedia API مع rate limit. يرجع {} لو فشل."""
    url = f"https://liquipedia.net/{wiki}/api.php"
    params = {**params, "format": "json", "maxlag": 5}
    try:
        time.sleep(LIQUIPEDIA_RATE_LIMIT_SEC)
        r = session.get(url, params=params, timeout=30)
        if r.status_code != 200:
            log.warning(f"Liquipedia {wiki} HTTP {r.status_code} ({params.get('action')})")
            return {}
        return r.json()
    except (requests.RequestException, ValueError) as e:
        log.warning(f"Liquipedia {wiki} request failed: {e}")
        return {}


def get_category_members(session: requests.Session, wiki: str, category: str) -> list:
    """يرجع كل عناوين الصفحات (namespace 0) داخل تصنيف معيّن، مع pagination."""
    members = []
    cmcontinue = None
    while True:
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": f"Category:{category}",
            "cmnamespace": 0,
            "cmlimit": 500,
            "cmtype": "page",
        }
        if cmcontinue:
            params["cmcontinue"] = cmcontinue
        data = _lp_get(session, wiki, params)
        query = data.get("query", {})
        batch = query.get("categorymembers", [])
        members.extend(p["title"] for p in batch if p.get("title"))
        cmcontinue = (data.get("continue") or {}).get("cmcontinue")
        if not cmcontinue:
            break
    return members


def fetch_wikitext(session: requests.Session, wiki: str, title: str) -> str:
    data = _lp_get(session, wiki, {
        "action": "query", "titles": title,
        "prop": "revisions", "rvprop": "content", "rvslots": "main",
    })
    pages = (data.get("query", {}) or {}).get("pages", {}) or {}
    for _pid, p in pages.items():
        revs = p.get("revisions") or []
        if not revs:
            continue
        slots = revs[0].get("slots", {})
        if slots:
            return slots.get("main", {}).get("*", "") or ""
        return revs[0].get("*", "") or ""
    return ""


# ============================================================
# Birth date extraction (pure, unit-testable, no network)
# ============================================================

# {{birth date|1998|5|12}} أو {{birth date and age|1998|5|12}} (وبأشكال
# تسمية قريبة يستخدمها بعض المحررين).
_TEMPLATE_BDATE_RE = re.compile(
    r"\{\{\s*[Bb]irth[\s_-]*date(?:[\s_-]*and[\s_-]*age)?\s*\|\s*"
    r"df\s*=\s*\w+\s*\|?\s*"          # بعض القوالب تحط df=yes/no أول
    r"(\d{4})\s*\|\s*(\d{1,2})\s*\|\s*(\d{1,2})",
)
_TEMPLATE_BDATE_RE_NO_DF = re.compile(
    r"\{\{\s*[Bb]irth[\s_-]*date(?:[\s_-]*and[\s_-]*age)?\s*\|\s*"
    r"(\d{4})\s*\|\s*(\d{1,2})\s*\|\s*(\d{1,2})",
)
# |birth_date=1998-05-12 أو |born=1998-05-12
_PLAIN_BDATE_RE = re.compile(
    r"\|\s*(?:birth[\s_-]*date|birthdate|born)\s*=\s*"
    r"(\d{4})-(\d{1,2})-(\d{1,2})",
    re.IGNORECASE,
)


def extract_birth_date(wikitext: str):
    """يرجع (year, month, day) كأرقام صحيحة، أو None لو ما لقى تاريخ صالح."""
    if not wikitext:
        return None
    for pattern in (_TEMPLATE_BDATE_RE, _TEMPLATE_BDATE_RE_NO_DF, _PLAIN_BDATE_RE):
        m = pattern.search(wikitext)
        if m:
            try:
                y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
                if 1 <= mo <= 12 and 1 <= d <= 31:
                    return (y, mo, d)
            except ValueError:
                continue
    return None


# ============================================================
# Crawl (network — used by `refresh` only)
# ============================================================

def crawl_all() -> list:
    """يمسح كل الويكيات × كل الدول العربية، ويرجع قائمة سجلات اللاعبين
    اللي لقى لهم تاريخ ميلاد صالح."""
    session = _lp_session()
    records = []
    seen_pages = set()  # (wiki, title) — تجنّب تكرار نفس اللاعب

    total_categories = len(GAME_WIKIS) * len(NATIONALITY_CATEGORY_CANDIDATES)
    done_categories = 0

    for wiki, game_label in GAME_WIKIS.items():
        for country_ar, candidates in NATIONALITY_CATEGORY_CANDIDATES.items():
            done_categories += 1
            members = []
            used_category = None
            for cat in candidates:
                members = get_category_members(session, wiki, cat)
                if members:
                    used_category = cat
                    break
            log.info(
                f"[{done_categories}/{total_categories}] {wiki} / {country_ar}: "
                f"{len(members)} لاعب (تصنيف: {used_category or 'غير موجود'})"
            )
            for title in members:
                key = (wiki, title)
                if key in seen_pages:
                    continue
                seen_pages.add(key)

                wikitext = fetch_wikitext(session, wiki, title)
                bdate = extract_birth_date(wikitext)
                if not bdate:
                    continue
                year, month, day = bdate
                records.append({
                    "name": title.replace("_", " "),
                    "wiki": wiki,
                    "game": game_label,
                    "country": country_ar,
                    "year": year,
                    "month": month,
                    "day": day,
                    "url": f"https://liquipedia.net/{wiki}/{title}",
                })

    log.info(f"تم: {len(records)} لاعب عندهم تاريخ ميلاد صالح من أصل {len(seen_pages)} صفحة مفحوصة.")
    return records


def save_cache(records: list) -> None:
    payload = {
        "generated_at": datetime.now(AMMAN_TZ).isoformat(),
        "count": len(records),
        "players": records,
    }
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    log.info(f"تم حفظ {len(records)} لاعب في {CACHE_FILE}")


def load_cache() -> dict:
    if not CACHE_FILE.exists():
        return {"generated_at": None, "count": 0, "players": []}
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def git_commit_push(paths: list, reason: str = "") -> None:
    """Commit + push the given files if they changed. Never raises — a
    failed push here should not crash the run, just gets retried next time."""
    try:
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"],
                        check=True, capture_output=True)
        subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"],
                        check=True, capture_output=True)
        subprocess.run(["git", "add", *paths], check=True, capture_output=True)
        diff = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if diff.returncode == 0:
            return  # nothing changed
        msg = "chore: update birthdays data [skip ci]"
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
# Daily check (no network to Liquipedia — reads cache only)
# ============================================================

def load_state() -> dict:
    if not STATE_FILE.exists():
        return {"sent": []}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"sent": []}
    data.setdefault("sent", [])
    return data


def save_state(state: dict) -> None:
    state["sent"] = state["sent"][-SENT_RING_SIZE:]
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def todays_matches(cache: dict, today: datetime) -> list:
    return [
        p for p in cache.get("players", [])
        if p["month"] == today.month and p["day"] == today.day
    ]


def send_birthday_discord(webhook_url: str, player: dict, today: datetime) -> bool:
    age = today.year - player["year"]
    description = (
        f"يحتفل {player['name']} اليوم بعيد ميلاده الـ{age}، "
        f"وهو يلعب حالياً في {player['game']}."
    )
    embed = {
        "title": f"عيد ميلاد {player['name']}",
        "description": description,
        "url": player["url"],
        "color": BIRTHDAY_EMBED_COLOR,
        "fields": [
            {"name": "اللعبة", "value": player["game"], "inline": True},
            {"name": "الجنسية", "value": player["country"], "inline": True},
        ],
    }
    payload = {"embeds": [embed]}
    for attempt in range(3):
        try:
            r = requests.post(webhook_url, json=payload, timeout=15)
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


def run_check(webhook_url: str) -> int:
    cache = load_cache()
    if not cache.get("players"):
        log.warning("players_birthdays.json فاضي أو غير موجود — شغّل `python birthdays.py refresh` أول.")
        return 0

    state = load_state()
    sent_set = set(state["sent"])

    today = datetime.now(AMMAN_TZ)
    date_key = today.strftime("%Y-%m-%d")

    matches = todays_matches(cache, today)
    sent_count = 0
    for player in matches:
        uid = f"{date_key}|{player['wiki']}|{player['name']}"
        if uid in sent_set:
            continue
        ok = send_birthday_discord(webhook_url, player, today)
        if ok:
            sent_count += 1
            sent_set.add(uid)
            state["sent"].append(uid)
            time.sleep(0.6)
        else:
            log.error(f"فشل إرسال عيد ميلاد {player['name']}")

    save_state(state)
    log.info(f"=== فحص اليوم ({date_key}): {len(matches)} تطابق، {sent_count} تم إرسالها ===")
    return sent_count


# ============================================================
# CLI
# ============================================================

def main():
    import os
    import sys

    if len(sys.argv) < 2 or sys.argv[1] not in ("refresh", "check"):
        print("الاستخدام: python birthdays.py refresh|check")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "refresh":
        records = crawl_all()
        save_cache(records)
        git_commit_push([str(CACHE_FILE)], reason="weekly refresh")
        return

    # mode == "check"
    webhook_url = (
        os.environ.get("BIRTHDAYS_WEBHOOK_URL", "").strip()
        or os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
    )
    if not webhook_url:
        log.error("Missing DISCORD_WEBHOOK_URL / BIRTHDAYS_WEBHOOK_URL env var")
        sys.exit(1)
    run_check(webhook_url)
    git_commit_push([str(STATE_FILE)], reason="daily check")


if __name__ == "__main__":
    main()
