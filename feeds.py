"""
GGNewsAR Bot — RSS Feed Configuration

131 English-language sources. No Arabic sources by design.

IMPORTANT: bot.py does NOT filter by "verified". Every source in this list
is attempted on every run, with no exception. "verified" is documentation
only — it tells you what happened in the last live test, nothing more.

Why dead links stay in the list (by design, per Hazem's instruction):
A feed that fails today (dead URL, site redesign, temporary outage) might
start working again later with zero code changes — the bot will pick it
up automatically the moment the URL responds with valid RSS/Atom again.
Removing it would mean losing that source permanently. The cost of
keeping it is one extra failed HTTP request per run; negligible.

Status from the live run on 2026-06-28 22:48 UTC (run #49):
  verified=True  -> confirmed returning real entries in that run.
  verified=False -> failed in that run (dead URL / wrong path / HTML
                     instead of XML / site down). Kept in the list on
                     purpose — will retry every run, no manual step needed
                     if it comes back online.

To manually fix a failing source: find the correct RSS path for that site
and update its url here. To check current status: look at "Failed Sources"
in the Actions log after any run.

UPDATE 2026-07-05: Fragster's three native RSS paths (/feed, /valorant/feed,
/overwatch/feed) were confirmed dead by direct fetch — all three silently
redirect to the homepage HTML instead of returning XML. The site itself is
alive and active (posts daily). Replaced all three with Google News
site: bridges instead of leaving them as dead native URLs.

UPDATE 2026-07-05 (priority field): each feed dict may include an optional
"priority" key: "high", "normal" (default if omitted), or "low". bot.py
processes and sends "high" priority sources first in every pass, before
"normal"/"low" ones -- so if the per-run send cap (MAX_MESSAGES_PER_RUN) is
ever hit, the important sources still get through.
NOTE (2026-08-11): this field was documented here since 2026-07-05 but
bot.py never actually sorted by it until today's update — it was a no-op
before. It is now genuinely applied. Mark a source "high" when it's a
primary/official account you want surfaced first: tournament organizers,
official team accounts, HLTV/VLR/Liquipedia-tier sources, etc.

UPDATE 2026-08-11 (region + source_type fields, comprehensive coverage pass):
  - "region": optional, "jordan" or "mena". Omit for global sources. This is
    only a starting hint passed to Gemini — the final region tag shown on
    each message is decided from the article's actual content, and Gemini
    is explicitly told to override the hint if the story doesn't match it.
  - "source_type": optional, "leak" for accounts/outlets that specialize in
    unconfirmed transfer rumors and insider reports rather than official
    announcements. Omit for normal confirmed-news sources (this is the
    default). Same override rule applies — a "leak" source publishing an
    official confirmed statement is still tagged "مؤكد", not "تسريب".
  - Added a dedicated Jordan section (was completely absent before, despite
    being GGNewsAR's home market — the MENA expansion block below covers
    every other Arab country but not Jordan itself).
  - Retagged known insider/rumor accounts already in this file (Sheep
    Esports, Slasher, RLewisReports, TravisGafford) as source_type="leak",
    and added KRL, a Counter-Strike-specific insider account with a strong
    public track record (accurately reported the karrigan-to-Falcons move
    ahead of IEM Cologne 2026, among others).
  - For a more structured, higher-signal leaks feed per game, see
    watchlist.py: every wiki now also watches that game's Liquipedia
    "Portal:Rumours" page (community-maintained, with its own
    Unlikely/Possible/Likely/Certain confidence scale per rumor).

UPDATE 2026-08-11 (round 3) — "loose_query" flag added after an unrelated
article slipped through:
  A bare Google News keyword bridge with no "site:" restriction and no
  quoted multi-word phrase (e.g. q=Syria+esports) is not a real esports
  feed — it is Google News's own relevance ranking, and for a "thin"
  scene with little genuine esports coverage it will pad the results
  with loosely related items that just happen to mention the country
  (a wildfire in Spain showed up under "Syria Esports" this way, since
  the query is really just "Syria" OR "esports" in practice, not an AND).
  "loose_query": True marks every source in this file built this way (the
  MENA per-country block, the general MENA/Jordan bridges, and the
  business/sponsorship keyword bridge). bot.py's Gemini relevance check
  (is_esports field) applies to every source regardless of this flag, but
  for "loose_query" sources specifically, a failed/unparseable Gemini
  call no longer falls back to sending the raw RSS item — it is dropped
  instead. For all other (already topic-restricted) sources, a raw
  fallback stays safe and is unchanged.

UPDATE 2026-08-11 (round 2) — Liquipedia removed entirely, leaks rebuilt,
broader coverage:
  - watchlist.py and everything Liquipedia-related is gone from bot.py per
    Hazem's explicit instruction (no Liquipedia content at all anymore).
    The "Portal:Rumours" leak mechanism mentioned in the note above no
    longer exists.
    [UPDATE 2026-08-22: watchlist.py, ggnewsar-transfers.patch, and three
    other orphaned files (ai_classifier.py, relevance.py, reddit_sources.py,
    transfers.py) were CLAIMED deleted here back on 2026-08-17, but that
    never actually happened — all six were still sitting in the repo,
    unused by bot.py or birthdays.py, contradicting this file's own
    docs ("RSS-only", "Liquipedia removed entirely"). Actually deleted
    now, for real this time.]
  - Leaks/rumors replacement: added one Google News bridge per major game
    that searches specifically for rumor-language keywords (rumor,
    reportedly, "in talks", leaked) rather than just general news. This is
    source-agnostic — it catches whichever outlet breaks a rumor first —
    instead of depending on a fixed list of named accounts. All tagged
    source_type="leak". Combined with the individual insider accounts
    already tagged the same way (Sheep Esports, Slasher, RLewisReports,
    TravisGafford, KRL).
  - Broader coverage: five more general sources added (Business of
    Esports, G2 Esports' own blog, Blog of Legends, Esports One Blog,
    Gamer Style Mexico) for more industry/analysis/regional breadth, on
    top of the existing 160+ sources.
  - The bot's run interval also changed from every 15 minutes to every 5
    hours (see bot.py's docstring) — this doesn't require any change in
    this file, but it does mean MAX_MESSAGES_PER_RUN in bot.py was raised
    since each pass now covers a much bigger accumulation window.

To add an X (Twitter) account as a source (e.g. official team or organizer
accounts, matching how wire-style accounts like @esports break news first):
X has no public RSS anymore, so bridge it through a self-hosted RSSHub
instance (the public rsshub.app is heavily rate-limited/blocked, not
reliable for production -- self-host one on Railway/Render/a small VPS,
it's free-tier friendly). Once you have your RSSHub URL, add entries like:
{"name": "HLTV (X)", "url": "https://YOUR-RSSHUB-INSTANCE/twitter/user/HLTV",
 "verified": False, "priority": "high"}
NOTE (2026-08-11): every X/Twitter entry below still points at the public
rsshub.app instance, which this same docstring has warned against since
2026-07-04. This is very likely why star-signing/transfer news (which
usually breaks on X first) has been under-covered — worth actually
self-hosting an RSSHub instance and swapping these URLs over.
"""

# ============================================================
# Scraper support (added 2026-08-22)
#
# Some sources don't expose a working RSS/Atom feed at all, so
# feedparser (XML-only) can never read them no matter what URL is
# tried. Sheep Esports is the first case: its old /rss endpoint is
# permanently dead (confirmed by fetching the site directly — it's
# JS-rendered HTML with no XML route since their Next.js relaunch),
# so it's scraped here instead.
#
# A source that needs this sets "fetch_type": "scraper" and
# "scraper": "<key>" in its RSS_FEEDS entry below (see the Sheep
# Esports entry). bot.py's fetch_one_feed() checks for that flag and
# calls SCRAPERS[<key>] instead of doing a normal RSS fetch — see the
# dispatch there for the exact contract.
#
# Every scraper function here must return the same 3-tuple
# fetch_one_feed() does: (name, entries_or_None, error_or_None). Never
# raise. Entries must be feedparser.FeedParserDict objects (or support
# the same .get()/getattr() access) so the rest of bot.py's pipeline —
# is_fresh(), extract_image(), Phase A/B/C — needs zero changes to
# consume them; that's the whole point of returning this exact shape
# instead of some new custom dict.
#
# IMPORTANT: this was written and unit-tested against synthetic HTML,
# but NOT run against the live site — the environment that built this
# has no network route to sheepesports.com. Run
# `python3 feeds.py --test-sheep` once after deploying to confirm it's
# actually pulling real entries (see the __main__ block at the bottom
# of this file).
# ============================================================
import re as _re
import json as _json
import html as _html_lib
from datetime import datetime as _datetime, timedelta as _timedelta, timezone as _timezone

import requests as _requests
import feedparser as _feedparser  # only used for FeedParserDict here, not .parse()

_SHEEP_BASE = "https://www.sheepesports.com"
# "Más recientes" / most-recent-first article listing (not the busy
# homepage, which mixes trending/interviews/leaks sections together).
_SHEEP_URL = f"{_SHEEP_BASE}/es/all/articles"

_NEXT_DATA_RE = _re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', _re.DOTALL
)
_ARTICLE_HREF_RE = _re.compile(
    r'href="(/[a-z]{2}/all/articles/[a-z0-9\-]+(?:/[a-z]{2})?)"', _re.IGNORECASE
)

# Recent items show relative ages ("59m", "2h", "1d", "1w"); older ones
# show absolute dates ("17.08.2026" = DD.MM.YYYY). Handle both.
_REL_TIME_RE = _re.compile(r'^(\d+)\s*(m|h|d|w)$')
_ABS_DATE_RE = _re.compile(r'^(\d{1,2})\.(\d{1,2})\.(\d{4})$')


def _sheep_parse_date(text: str):
    """Best-effort parse into a time.struct_time (what feedparser puts
    in published_parsed). Returns None on anything unrecognized —
    bot.py's is_fresh() treats a missing date as always-fresh, so this
    fails safe rather than dropping an entry."""
    text = (text or "").strip().lower()
    m = _REL_TIME_RE.match(text)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        delta = {
            "m": _timedelta(minutes=n),
            "h": _timedelta(hours=n),
            "d": _timedelta(days=n),
            "w": _timedelta(weeks=n),
        }[unit]
        return (_datetime.now(_timezone.utc) - delta).timetuple()
    m = _ABS_DATE_RE.match(text)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return _datetime(year, month, day, tzinfo=_timezone.utc).timetuple()
        except ValueError:
            return None
    return None


def _sheep_make_entry(title: str, link: str, summary: str = "", date_text: str = "", image: str = ""):
    e = _feedparser.FeedParserDict()
    e["title"] = title.strip()
    e["link"] = link
    e["summary"] = summary
    pub = _sheep_parse_date(date_text)
    if pub:
        e["published_parsed"] = pub
    if image:
        e["media_content"] = [{"url": image}]
    return e


def _sheep_looks_like_article(d) -> bool:
    if not isinstance(d, dict):
        return False
    keys = {k.lower() for k in d.keys()}
    has_title = any(k in keys for k in ("title", "headline", "name"))
    has_link = any(k in keys for k in ("slug", "url", "href", "path"))
    return has_title and has_link


def _sheep_find_article_list(node, depth: int = 0):
    """Walk the __NEXT_DATA__ props tree looking for the first list of
    dicts that looks like articles. Shallow depth cap so this can't
    loop on a pathological/circular structure."""
    if depth > 6:
        return None
    if isinstance(node, list) and node and all(_sheep_looks_like_article(i) for i in node[:3]):
        return node
    if isinstance(node, dict):
        for v in node.values():
            found = _sheep_find_article_list(v, depth + 1)
            if found:
                return found
    return None


def _sheep_from_next_data(html_text: str):
    """Preferred path: pull the exact article list Next.js used to
    server-render the page out of the __NEXT_DATA__ JSON blob, instead
    of trying to re-derive it from rendered/linearized text."""
    m = _NEXT_DATA_RE.search(html_text)
    if not m:
        return None
    try:
        data = _json.loads(m.group(1))
    except _json.JSONDecodeError:
        return None

    page_props = data.get("props", {}).get("pageProps", {}) if isinstance(data, dict) else {}
    articles = _sheep_find_article_list(page_props)
    if not articles:
        return None

    entries = []
    for a in articles:
        title = a.get("title") or a.get("headline") or a.get("name") or ""
        slug = a.get("slug") or a.get("url") or a.get("href") or a.get("path") or ""
        if not title or not slug:
            continue
        link = slug if str(slug).startswith("http") else f"{_SHEEP_BASE}{slug if str(slug).startswith('/') else '/' + slug}"
        summary = a.get("excerpt") or a.get("description") or a.get("summary") or ""
        date_text = str(a.get("publishedAt") or a.get("date") or a.get("createdAt") or "")
        image = a.get("image") or a.get("thumbnail") or ""
        if isinstance(image, dict):
            image = image.get("url", "")
        entries.append(_sheep_make_entry(title, link, summary, date_text, image))
    return entries or None


def _sheep_from_html_fallback(html_text: str):
    """Fallback if __NEXT_DATA__ isn't found or doesn't match the
    expected shape: regex out every /all/articles/ link and recover a
    title from the anchor's own text. Cruder — rendered text on this
    site mixes image credit lines, tag pills, and the real headline
    together, so titles from this path can be noisy. Last resort."""
    seen_links = set()
    entries = []
    for href_match in _ARTICLE_HREF_RE.finditer(html_text):
        href = href_match.group(1)
        link = f"{_SHEEP_BASE}{href}"
        if link in seen_links:
            continue
        seen_links.add(link)

        tail = html_text[href_match.end():]
        tag_end = tail.find(">")  # rest of the <a ...> tag's other attrs, if any
        if tag_end == -1:
            continue
        tail = tail[tag_end + 1:]
        close = tail.find("</a>")
        if close == -1:
            continue
        inner_html = tail[:close]
        inner_text = _html_lib.unescape(_re.sub(r"<[^>]+>", " ", inner_html))
        inner_text = _re.sub(r"\s+", " ", inner_text).strip()

        slug_title = href.rstrip("/").split("/")[-1]
        if slug_title in ("en", "es", "fr"):
            slug_title = href.rstrip("/").split("/")[-2]
        fallback_title = slug_title.replace("-", " ").capitalize()

        title = inner_text if len(inner_text) > 8 else fallback_title
        entries.append(_sheep_make_entry(title, link))
    return entries


def fetch_sheep_esports(feed_info: dict):
    """Same return contract as bot.py's fetch_one_feed:
    (name, entries_or_None, error_or_None). Never raises."""
    name = feed_info["name"]
    url = feed_info.get("url") or _SHEEP_URL
    try:
        resp = _requests.get(
            url,
            timeout=15,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            },
        )
        resp.raise_for_status()
        html_text = resp.text

        entries = _sheep_from_next_data(html_text)
        source = "next_data"
        if not entries:
            entries = _sheep_from_html_fallback(html_text)
            source = "html_fallback"

        if not entries:
            raise RuntimeError("no entries (checked __NEXT_DATA__ and HTML fallback)")

        for e in entries:
            e["_sheep_scrape_source"] = source  # debug only, harmless downstream

        return name, entries, None
    except Exception as e:
        return name, None, str(e)


# Registry bot.py's fetch_one_feed() dispatches into for any source
# marked "fetch_type": "scraper". Add new scraped sources here as they
# come up — key is whatever string you put in that source's "scraper"
# field below.
SCRAPERS = {
    "sheep_esports": fetch_sheep_esports,
}


RSS_FEEDS = [
    # ============================================================
    # Confirmed working (live run #49, 2026-06-28)
    # ============================================================
    {"name": "HLTV", "url": "https://www.hltv.org/rss/news", "verified": True},
    {"name": "VLR.gg", "url": "https://vlr.gg/rss", "verified": True},
    {"name": "Dotabuff Blog", "url": "https://www.dotabuff.com/blog.rss", "verified": True},
    {"name": "Dot Esports", "url": "https://dotesports.com/feed", "verified": True},
    {"name": "Esports Insider", "url": "https://esportsinsider.com/feed", "verified": True},
    {"name": "ESTNN", "url": "https://estnn.com/feed", "verified": True},
    {"name": "Esports News UK", "url": "https://esports-news.co.uk/feed", "verified": True},
    {"name": "Insider Gaming", "url": "https://insider-gaming.com/feed", "verified": True},
    {"name": "The Esports Radar", "url": "https://esportsradar.gg/feed", "verified": True},
    {"name": "Esports.gg", "url": "https://esports.gg/feed", "verified": True},
    {"name": "The Loadout", "url": "https://www.theloadout.com/feed", "verified": True},
    {"name": "TalkEsport", "url": "https://talkesport.com/feed", "verified": True},
    {"name": "The Game Haus", "url": "https://thegamehaus.com/feed", "verified": True},
    {"name": "Dexerto Esports", "url": "https://www.dexerto.com/esports/feed", "verified": True},
    {"name": "DBLTap", "url": "https://www.dbltap.com/feed", "verified": True},
    {"name": "Esports.net", "url": "https://www.esports.net/feed", "verified": True},
    {"name": "GameRiv", "url": "https://gameriv.com/feed", "verified": True},
    {"name": "Esports Wizard", "url": "https://esportswizard.com/feed", "verified": True},
    {"name": "Esports Group", "url": "https://esportsgroup.net/feed", "verified": True},
    {"name": "CS Spy", "url": "https://csspy.com/feed", "verified": True},
    {"name": "Counter-Strike Official Blog", "url": "https://blog.counter-strike.net/index.php/feed", "verified": True},
    {"name": "GameRiv Valorant", "url": "https://gameriv.com/valorant/feed", "verified": True},
    {"name": "Nerfplz", "url": "https://www.nerfplz.com/feeds/posts/default", "verified": True},
    {"name": "Surrender at 20", "url": "https://feeds.feedburner.com/Surrenderat20", "verified": True},
    {"name": "ESTNN LoL", "url": "https://estnn.com/tag/league-of-legends/feed", "verified": True},
    {"name": "DotaBlast", "url": "https://dotablast.com/feed", "verified": True},
    {"name": "ESTNN Overwatch", "url": "https://estnn.com/tag/overwatch-esports/feed", "verified": True},
    {"name": "MP1st CoD", "url": "https://mp1st.com/tag/call-of-duty/feed", "verified": True},
    {"name": "Global Esports News CoD", "url": "https://global-esports.news/category/call-of-duty/feed", "verified": True},
    {"name": "Esports Wizard Apex", "url": "https://esportswizard.com/news/tag/apex-legends/feed", "verified": True},
    {"name": "Dexerto Apex", "url": "https://www.dexerto.com/apex-legends/feed", "verified": True},
    {"name": "The Loadout PUBG", "url": "https://www.theloadout.com/pubg/feed", "verified": True},
    {"name": "Esports Advocate", "url": "https://esportsadvocate.net/feed", "verified": True},
    {"name": "Esports Wales", "url": "https://esportswales.org/feed", "verified": True},
    {"name": "GRID Esports Data Blog", "url": "https://blog.grid.gg/feed", "verified": True},
    {"name": "Traxion.gg Esports", "url": "https://traxion.gg/category/esports/feed", "verified": True},
    {"name": "Esports Inquirer", "url": "https://esports.inquirer.net/feed", "verified": True},
    {"name": "RealSport101", "url": "https://realsport101.com/feed.xml", "verified": True},

    # ============================================================
    # Failed in run #49 — kept in the list, retried every run.
    # Will start working automatically if the URL becomes valid again.
    # ============================================================
    {"name": "Esports Talk", "url": "https://esportstalk.com/feed", "verified": False},
    {"name": "Snowball Esports", "url": "https://snowballesports.com/feed", "verified": False},
    {"name": "AFK Gaming", "url": "https://afkgaming.com/rssfeed", "verified": False},
    {"name": "WIN.gg", "url": "https://win.gg/feed", "verified": False},
    {"name": "GosuGamers", "url": "https://www.gosugamers.net/feed", "verified": False},
    {"name": "Esports.com", "url": "https://www.esports.com/en/feed", "verified": False},
    {"name": "Fragster (via Google News bridge, native feed dead)", "url": "https://news.google.com/rss/search?q=site:fragster.com&hl=en&gl=US&ceid=US:en", "verified": False},
    {"name": "Hotspawn", "url": "https://www.hotspawn.com/feed", "verified": False},
    {"name": "G2G News Esports", "url": "https://g2g.news/feed", "verified": False},
    {"name": "ONE Esports", "url": "https://www.oneesports.gg/feed", "verified": False},
    {"name": "Way to Smurf", "url": "https://www.waytosmurf.com/feed", "verified": False},
    {"name": "UKCSGO", "url": "https://ukcsgo.com/feed", "verified": False},
    {"name": "CSGO2ASIA", "url": "https://csgo2asia.com/feed", "verified": False},
    {"name": "Esports Talk CS", "url": "https://esportstalk.com/blog/csgo/feed", "verified": False},
    {"name": "Esports Talk Valorant", "url": "https://esportstalk.com/blog/valorant/feed", "verified": False},
    {"name": "Esports.net Valorant", "url": "https://www.esports.net/news/valorant/feed", "verified": False},
    {"name": "Fragster Valorant (via Google News bridge, native feed dead)", "url": "https://news.google.com/rss/search?q=site:fragster.com+valorant&hl=en&gl=US&ceid=US:en", "verified": False},
    {"name": "ValorantInfo.gg", "url": "https://valorantinfo.gg/feed", "verified": False},
    {"name": "DBLTap Valorant", "url": "https://www.dbltap.com/leagues/valorant/feed", "verified": False},
    {"name": "LoL News", "url": "https://lolnews.com/feed", "verified": False},
    {"name": "Snowball LoL", "url": "https://snowballesports.com/games/league-of-legends/feed", "verified": False},
    {"name": "Esports Talk LoL", "url": "https://esportstalk.com/blog/league-of-legends/feed", "verified": False},
    {"name": "Escorenews LoL", "url": "https://escorenews.com/en/lol/feed", "verified": False},
    {"name": "ONE Esports Dota 2", "url": "https://www.oneesports.gg/dota2/feed", "verified": False},
    {"name": "Esports.net Dota", "url": "https://www.esports.net/news/dota/feed", "verified": False},
    {"name": "Sportskeeda Dota 2", "url": "https://www.sportskeeda.com/esports/dota-2/feed", "verified": False},
    {"name": "Esports.com Dota 2", "url": "https://www.esports.com/en/dota-2/feed", "verified": False},
    {"name": "WIN.gg Dota 2", "url": "https://win.gg/dota2/feed", "verified": False},
    {"name": "Fragster Overwatch (via Google News bridge, native feed dead)", "url": "https://news.google.com/rss/search?q=site:fragster.com+overwatch&hl=en&gl=US&ceid=US:en", "verified": False},
    {"name": "Esports Talk Overwatch", "url": "https://esportstalk.com/blog/overwatch/feed", "verified": False},
    {"name": "DBLTap Overwatch", "url": "https://www.dbltap.com/leagues/overwatch/feed", "verified": False},
    {"name": "Hotspawn Overwatch", "url": "https://www.hotspawn.com/overwatch/news/feed", "verified": False},
    {"name": "Esports Talk CoD", "url": "https://esportstalk.com/blog/call-of-duty/feed", "verified": False},
    {"name": "ONE Esports CoD", "url": "https://www.oneesports.gg/call-of-duty/feed", "verified": False},
    {"name": "ONE Esports Apex", "url": "https://www.oneesports.gg/apex-legends/feed", "verified": False},
    {"name": "Dot Esports PUBG", "url": "https://dotesports.com/pubg/feed", "verified": False},
    {"name": "Esports Talk PUBG Mobile", "url": "https://esportstalk.com/news/pubg-mobile/feed", "verified": False},
    {"name": "DBLTap PUBG", "url": "https://www.dbltap.com/leagues/pubg/feed", "verified": False},
    {"name": "Esports.net Mobile Games", "url": "https://www.esports.net/news/mobile-games/feed", "verified": False},
    {"name": "RLRSS", "url": "https://rlrss.qrivi.dev/feed", "verified": False},
    {"name": "EventHubs", "url": "https://www.eventhubs.com/feed/", "verified": False},
    {"name": "AFK Gaming Alt", "url": "https://afkgaming.com/feed", "verified": False},
    {"name": "InsideSport Esports", "url": "https://insidesport.in/topic/esports/feed", "verified": False},
    {"name": "Esports Insider (alt path)", "url": "https://esportsinsider.com/news/feed", "verified": False},
    {"name": "The Esports Observer Archive", "url": "https://esportsobserver.com/feed", "verified": False},
    {"name": "Esports.net Rainbow Six", "url": "https://www.esports.net/news/rainbow-six/feed", "verified": False},
    {"name": "Strafe Valorant", "url": "https://www.strafe.com/news/valorant/feed", "verified": False},
    {"name": "Strafe R6S", "url": "https://www.strafe.com/news/r6s/feed", "verified": False},
    {"name": "Strafe General", "url": "https://www.strafe.com/news/feed", "verified": False},
    {"name": "SiegeGG News", "url": "https://siege.gg/news/feed", "verified": False},
    {"name": "Philstar Esports", "url": "https://www.philstar.com/esport/news/feed", "verified": False},
    {"name": "GGRecon", "url": "https://www.ggrecon.com/feed", "verified": False},
    {"name": "PC Invasion Esports", "url": "https://www.pcinvasion.com/category/esports/feed", "verified": False},
    {"name": "Sportskeeda Esports", "url": "https://www.sportskeeda.com/esports/feed", "verified": False},
    {"name": "Esports Betting News", "url": "https://esportsbets.com/feed", "verified": False},
    {"name": "Esports Talk CS2 Alt", "url": "https://esportstalk.com/news/csgo/feed", "verified": False},
    {"name": "Escorenews CS2", "url": "https://escorenews.com/en/cs2/feed", "verified": False},
    {"name": "Mobalytics Valorant", "url": "https://mobalytics.gg/blog/valorant/feed", "verified": False},
    {"name": "Esports Talk Dota2", "url": "https://esportstalk.com/blog/dota-2/feed", "verified": False},
    {"name": "WIN.gg LoL", "url": "https://win.gg/lol/feed", "verified": False},

    # ============================================================
    # Business / sponsorship / investment / industry coverage
    # Added 2026-06-29 per request to cover esports business news
    # (deals, sponsorships, investment) from specialized and general
    # industry trade press, not just match/tournament news.
    # ============================================================
    {"name": "GamesIndustry.biz", "url": "https://www.gamesindustry.biz/rss/gamesindustry_news_feed.rss", "verified": False},
    {"name": "SK Gaming", "url": "https://sk-gaming.com/news/rss.xml", "verified": False},
    {"name": "Esportstower", "url": "https://esportstower.com/feed", "verified": False},
    {"name": "SportsPro Esports", "url": "https://www.sportspromedia.com/tag/esports/feed", "verified": False},
    {"name": "Challengermode Blog", "url": "https://blog.challengermode.com/feed", "verified": False},
    {"name": "F1 Esports", "url": "https://f1esports.com/news/feed", "verified": False},
    {"name": "NESTHQ", "url": "https://nesthq.ca/feed", "verified": False},
    {"name": "Esports Charts News", "url": "https://escharts.com/news/feed", "verified": False},

    # ============================================================
    # Added 2026-07-01 per Hazem's request. Sheep Esports is explicitly
    # a leaks/rumors outlet (LoL, VALORANT, CS2, Rocket League) —
    # retagged 2026-08-11.
    #
    # UPDATE 2026-08-22: /rss above was confirmed permanently dead, not
    # just temporarily down — the site relaunched on Next.js at some
    # point and no longer exposes any RSS/Atom route at all (confirmed
    # by fetching the page directly: it's JS-rendered HTML, not XML,
    # so feedparser always returns 0 entries no matter which path is
    # tried). Switched this source to a scraper (sheep_scraper.py)
    # instead of an RSS url — see fetch_type/scraper below. bot.py's
    # fetch_one_feed() dispatches to it automatically.
    # ============================================================
    {
        "name": "Sheep Esports",
        "url": "https://www.sheepesports.com/es/all/articles",
        "verified": False,
        "source_type": "leak",
        "fetch_type": "scraper",
        "scraper": "sheep_esports",
    },

    # ============================================================
    # 2026-07-01, batch 3 — cleanup pass.
    # ============================================================
    {"name": "GamingOnPhone Esports", "url": "https://gamingonphone.com/category/esports/feed", "verified": False},

    # ============================================================
    # Added 2026-07-01, batch 2
    # ============================================================
    {"name": "Esports Marketing Blog", "url": "https://esports-marketing-blog.com/feed/", "verified": False},
    {"name": "European Gaming Media", "url": "https://europeangaming.eu/portal/feed/", "verified": False},
    {"name": "Esports Africa News", "url": "https://esportsafricanews.com/feed/", "verified": False},
    {"name": "LoL Esports (official, via Google News)", "url": "https://news.google.com/rss/search?q=site:lolesports.com&hl=en&gl=US&ceid=US:en", "verified": False},
    {"name": "VALORANT Esports (official, via Google News)", "url": "https://news.google.com/rss/search?q=site:valorantesports.com&hl=en&gl=US&ceid=US:en", "verified": False},
    {"name": "Esports World Cup (official, via Google News)", "url": "https://news.google.com/rss/search?q=site:esportsworldcup.com&hl=en&gl=US&ceid=US:en", "verified": False},
    {"name": "Rocket League Esports (official, via Google News)", "url": "https://news.google.com/rss/search?q=site:rocketleagueesports.com&hl=en&gl=US&ceid=US:en", "verified": False},
    {"name": "PUBG Esports (official, via Google News)", "url": "https://news.google.com/rss/search?q=site:pubgesports.com&hl=en&gl=US&ceid=US:en", "verified": False},
    {"name": "ALGS - Apex Legends (official, via Google News)", "url": "https://news.google.com/rss/search?q=site:algs.com&hl=en&gl=US&ceid=US:en", "verified": False},
    {"name": "Call of Duty League (official, via Google News)", "url": "https://news.google.com/rss/search?q=site:callofdutyleague.com&hl=en&gl=US&ceid=US:en", "verified": False},
    {"name": "Esports Business & Sponsorships (Google News bridge)", "url": "https://news.google.com/rss/search?q=esports+(sponsorship+OR+partnership+OR+investment+OR+acquisition+OR+revenue)&hl=en&gl=US&ceid=US:en", "verified": False, "loose_query": True},

    # ============================================================
    # REMOVED 2026-08-23 — the entire "X via public rsshub.app" block
    # (24 entries: Team Falcons, Twisted Minds, Nigma Galaxy, Geekay,
    # FATE, EWC, ESL, BLAST, PGL, IEM, CS, VALORANT, LoL, Dota, R6,
    # Rocket League, PUBG, MLBB, EA FC, plus the insider/leak accounts
    # Slasher, RLewisReports, TravisGafford, KRL_STREAM).
    #
    # Why: Hazem reported the news-bot Discord channel posting unrelated
    # spam — a "join our Telegram channel" ad for a bot called
    # "A-TOOLS X" — with the exact formatting of a Discord auto-unfurled
    # link preview (i.e. the message that produced it was a bare t.me
    # URL, not anything this codebase's Arabic package generator or
    # Gemini prompt ever produces — grepped the whole repo, that string
    # exists nowhere in the source). This file's own docstring has warned
    # since 2026-07-04 that "the public rsshub.app is heavily
    # rate-limited/blocked, not reliable for production" — a shared free
    # demo proxy is also the one place in this pipeline where the actual
    # HTTP response body is entirely outside our control, since it's a
    # live third party passing through whatever it returns for a broken
    # route (ad-injected error page, hijacked/expired route, etc.), not a
    # dedicated RSS/Atom feed we've inspected. It's also the single
    # biggest reliability drag on this list: "verified" was False for
    # every single one of these 24 entries, meaning by this file's own
    # tracking none of them were confirmed actually returning real
    # content as of the last live test — 24 dead-weight fetches per run
    # for zero delivered news, on top of the spam risk.
    #
    # Net effect of removing them: no loss of real coverage (they weren't
    # verified as working anyway), one less unvetted third-party proxy in
    # the pipeline, and no more plausible entry point for this kind of ad
    # content to reach Discord.
    #
    # To get these accounts back safely: self-host a private RSSHub
    # instance (Railway/Render free tier works) so the response is one
    # you control, then re-add entries pointing at your own instance —
    # see the "To add an X (Twitter) account" note in this file's
    # top docstring for the exact entry shape.

    # ============================================================
    # MENA EXPANSION — added 2026-07-05: full MENA coverage (Gulf,
    # Levant, Egypt, North Africa) + regional tournaments. Google
    # News RSS bridge, since none of these federations/teams publish
    # native RSS. Confidence tags: [checked] confirmed active team/
    # program, [federation] official body not a competing team,
    # [thin] no confirmed organized scene found.
    # All entries below tagged region="mena" (2026-08-11).
    # ============================================================
    {"name": "Kuwait Esports Club [federation]", "url": "https://news.google.com/rss/search?q=%22Kuwait+Esports%22&hl=en&gl=US&ceid=US:en", "verified": False, "region": "mena", "loose_query": True},
    {"name": "Bahrain Esports [checked, ENC26 national team]", "url": "https://news.google.com/rss/search?q=Bahrain+esports&hl=en&gl=US&ceid=US:en", "verified": False, "region": "mena", "loose_query": True},
    {"name": "Qatar Esports [thin]", "url": "https://news.google.com/rss/search?q=Qatar+esports&hl=en&gl=US&ceid=US:en", "verified": False, "region": "mena", "loose_query": True},
    {"name": "Oman Esports [checked, ENC26 national team]", "url": "https://news.google.com/rss/search?q=Oman+esports&hl=en&gl=US&ceid=US:en", "verified": False, "region": "mena", "loose_query": True},
    {"name": "Lebanon Esports [checked, ENC26 national team]", "url": "https://news.google.com/rss/search?q=Lebanon+esports&hl=en&gl=US&ceid=US:en", "verified": False, "region": "mena", "loose_query": True},
    {"name": "Syria Esports [checked, ENC26 national team]", "url": "https://news.google.com/rss/search?q=Syria+esports&hl=en&gl=US&ceid=US:en", "verified": False, "region": "mena", "loose_query": True},
    {"name": "Palestine Esports [checked, ENC26 national team]", "url": "https://news.google.com/rss/search?q=Palestine+esports&hl=en&gl=US&ceid=US:en", "verified": False, "region": "mena", "loose_query": True},
    {"name": "Iraq Esports [checked, ENC26 national team, top 4 finish]", "url": "https://news.google.com/rss/search?q=Iraq+esports&hl=en&gl=US&ceid=US:en", "verified": False, "region": "mena", "loose_query": True},
    {"name": "Morocco Esports [checked, ENC26 MENA runner-up]", "url": "https://news.google.com/rss/search?q=Morocco+esports&hl=en&gl=US&ceid=US:en", "verified": False, "region": "mena", "loose_query": True},
    {"name": "Algeria Esports [checked, ENC26 national team]", "url": "https://news.google.com/rss/search?q=Algeria+esports&hl=en&gl=US&ceid=US:en", "verified": False, "region": "mena", "loose_query": True},
    {"name": "Tunisia Esports [checked, ENC26 MENA 3rd place]", "url": "https://news.google.com/rss/search?q=Tunisia+esports&hl=en&gl=US&ceid=US:en", "verified": False, "region": "mena", "loose_query": True},
    {"name": "Libya Esports [thin]", "url": "https://news.google.com/rss/search?q=Libya+esports&hl=en&gl=US&ceid=US:en", "verified": False, "region": "mena", "loose_query": True},
    {"name": "Mauritania Esports [thin]", "url": "https://news.google.com/rss/search?q=Mauritania+esports&hl=en&gl=US&ceid=US:en", "verified": False, "region": "mena", "loose_query": True},
    {"name": "Sudan Esports [thin]", "url": "https://news.google.com/rss/search?q=Sudan+esports&hl=en&gl=US&ceid=US:en", "verified": False, "region": "mena", "loose_query": True},
    {"name": "Al-Ahli Esports (Saudi, new CS2 roster June 2026) [checked]", "url": "https://news.google.com/rss/search?q=%22Al+Ahli%22+esports+CS2&hl=en&gl=US&ceid=US:en", "verified": False, "region": "mena", "loose_query": True},
    {"name": "Esports Nations Cup 2026 [checked, EWCF national-team series]", "url": "https://news.google.com/rss/search?q=%22Esports+Nations+Cup%22&hl=en&gl=US&ceid=US:en", "verified": False, "region": "mena", "loose_query": True},
    {"name": "MENA Esports general", "url": "https://news.google.com/rss/search?q=MENA+esports+OR+%22Middle+East%22+esports&hl=en&gl=US&ceid=US:en", "verified": False, "region": "mena", "loose_query": True},

    # ============================================================
    # JORDAN — added 2026-08-11. Was completely missing before despite
    # being GGNewsAR's home market; the MENA expansion block above covers
    # every other Arab country but never included Jordan itself. Official
    # body is the Jordan Esports Federation (JEF, jef.jo, X: @Jordan_Esports,
    # est. as a federation August 2023). No native RSS from JEF's own site,
    # so bridged the same way as the rest of the MENA block.
    # ============================================================
    {"name": "Jordan Esports Federation (official, via Google News)", "url": "https://news.google.com/rss/search?q=site:jef.jo&hl=en&gl=US&ceid=US:en", "verified": False, "region": "jordan", "priority": "high"},
    {"name": "Jordan Esports general", "url": "https://news.google.com/rss/search?q=%22Jordan+Esports%22+OR+%22Jordan+Esports+Federation%22&hl=en&gl=US&ceid=US:en", "verified": False, "region": "jordan", "priority": "high", "loose_query": True},
    # REMOVED 2026-08-23 — same public-rsshub.app removal as above; see
    # that block's note for why. High-priority Jordan coverage still
    # comes through the two Google News bridges right above.

    # ============================================================
    # BROADER COVERAGE — added 2026-08-11 per Hazem's request for more
    # expansive coverage: additional general sources (more RSS across
    # existing games/regions) plus outlets covering industry/business/
    # analysis rather than just breaking match news. Sourced from
    # FeedSpot's esports RSS directory, cross-checked against the list
    # already above to avoid duplicates.
    # ============================================================
    {"name": "Business of Esports", "url": "https://thebusinessofesports.com/feed/", "verified": False},
    {"name": "G2 Esports (official team blog)", "url": "https://g2esports.com/blogs/news.atom", "verified": False, "priority": "high"},
    {"name": "Blog of Legends (LoL news, rumors, updates)", "url": "https://blogoflegends.com/league-of-legends-esports/feed/", "verified": False},
    {"name": "Esports One Blog", "url": "https://blog.esportsone.com/feed/", "verified": False},
    {"name": "Gamer Style Mexico (Esports, LatAm)", "url": "https://gamerstyle.com.mx/category/esports/feed/", "verified": False},

    # ============================================================
    # ADDED 2026-08-27 per Hazem's request — Pley.gg plus similar
    # multi-game outlets that focus specifically on team/player news
    # (roster moves, interviews, tournament storylines), the same
    # niche Pley.gg covers for CS2/Valorant/Apex. None of these have
    # been live-tested from this environment (no network route to
    # them here), so all are added "verified": False per this file's
    # standard practice — the bot will confirm/retry automatically.
    # ============================================================
    {"name": "Pley.gg (CS2, Valorant, Apex Legends)", "url": "https://pley.gg/feed", "verified": False},
    {"name": "EGamersWorld (roster/team news, multi-game)", "url": "https://egamersworld.com/feed", "verified": False},
    {"name": "Millenium/MGG (English, multi-game esports)", "url": "https://us.millenium.gg/feed", "verified": False},
    # REMOVED 2026-08-27 (same day, minutes after being added) — "Mobile
    # Wins" was added here under the mistaken belief it was a gaming news
    # outlet ("LoL, CS2, CoD news"). It is actually mobilewins.co.uk, a UK
    # online casino/sports-betting affiliate site with no esports news
    # content at all. Its /feed is exactly the kind of unvetted third-party
    # content this file's docstring already warns about (see the 2026-08-23
    # rsshub.app removal note above): affiliate/casino blogs routinely stuff
    # their RSS with promotional ad content, which is what reached the
    # Discord channel as a "join our Telegram channel — A-TOOLS X" ad within
    # a minute of this entry being added. Do not re-add unless the site is
    # actually confirmed to be an esports news source.

    # ============================================================
    # LEAKS/RUMORS — REBUILT 2026-08-11 after removing Liquipedia
    # entirely (its "Portal:Rumours" pages, which used to cover this,
    # are gone along with the rest of the Liquipedia integration).
    # Replacement approach: targeted Google News bridges per game,
    # searching specifically for rumor-language keywords (rumor,
    # reportedly, in talks, leaked) rather than just general news, using
    # the same Google News bridge pattern already proven elsewhere in
    # this file. This is source-agnostic (catches whichever outlet
    # breaks a given rumor first) instead of depending on one or two
    # named accounts. Combined with the insider accounts already tagged
    # source_type="leak" above (Sheep Esports, Slasher, RLewisReports,
    # TravisGafford, KRL).
    # ============================================================
    {"name": "CS2 rumors (Google News bridge)", "url": "https://news.google.com/rss/search?q=(%22Counter-Strike+2%22+OR+CS2)+(rumor+OR+rumour+OR+reportedly+OR+%22in+talks%22+OR+leaked)&hl=en&gl=US&ceid=US:en", "verified": False, "source_type": "leak"},
    {"name": "VALORANT rumors (Google News bridge)", "url": "https://news.google.com/rss/search?q=VALORANT+esports+(rumor+OR+rumour+OR+reportedly+OR+%22in+talks%22+OR+leaked)&hl=en&gl=US&ceid=US:en", "verified": False, "source_type": "leak"},
    {"name": "League of Legends rumors (Google News bridge)", "url": "https://news.google.com/rss/search?q=%22League+of+Legends%22+esports+(rumor+OR+rumour+OR+reportedly+OR+%22in+talks%22+OR+leaked)&hl=en&gl=US&ceid=US:en", "verified": False, "source_type": "leak"},
    {"name": "Dota 2 rumors (Google News bridge)", "url": "https://news.google.com/rss/search?q=%22Dota+2%22+(rumor+OR+rumour+OR+reportedly+OR+%22in+talks%22+OR+leaked)&hl=en&gl=US&ceid=US:en", "verified": False, "source_type": "leak"},
    {"name": "Mobile Legends rumors (Google News bridge)", "url": "https://news.google.com/rss/search?q=%22Mobile+Legends%22+(rumor+OR+rumour+OR+reportedly+OR+leaked)&hl=en&gl=US&ceid=US:en", "verified": False, "source_type": "leak"},
    {"name": "PUBG Mobile rumors (Google News bridge)", "url": "https://news.google.com/rss/search?q=%22PUBG+Mobile%22+esports+(rumor+OR+rumour+OR+reportedly+OR+leaked)&hl=en&gl=US&ceid=US:en", "verified": False, "source_type": "leak"},
    {"name": "Rainbow Six rumors (Google News bridge)", "url": "https://news.google.com/rss/search?q=%22Rainbow+Six%22+esports+(rumor+OR+rumour+OR+reportedly+OR+leaked)&hl=en&gl=US&ceid=US:en", "verified": False, "source_type": "leak"},
    {"name": "Rocket League rumors (Google News bridge)", "url": "https://news.google.com/rss/search?q=%22Rocket+League%22+esports+(rumor+OR+rumour+OR+reportedly+OR+leaked)&hl=en&gl=US&ceid=US:en", "verified": False, "source_type": "leak"},
    {"name": "Overwatch rumors (Google News bridge)", "url": "https://news.google.com/rss/search?q=Overwatch+esports+(rumor+OR+rumour+OR+reportedly+OR+leaked)&hl=en&gl=US&ceid=US:en", "verified": False, "source_type": "leak"},
    {"name": "Call of Duty rumors (Google News bridge)", "url": "https://news.google.com/rss/search?q=%22Call+of+Duty%22+esports+(rumor+OR+rumour+OR+reportedly+OR+leaked)&hl=en&gl=US&ceid=US:en", "verified": False, "source_type": "leak"},
    {"name": "MENA esports transfer rumors (Google News bridge)", "url": "https://news.google.com/rss/search?q=(%22Team+Falcons%22+OR+%22Twisted+Minds%22+OR+%22Nigma+Galaxy%22+OR+%22Geekay+Esports%22+OR+MENA+esports)+(rumor+OR+rumour+OR+reportedly+OR+%22in+talks%22+OR+leaked)&hl=en&gl=US&ceid=US:en", "verified": False, "source_type": "leak", "region": "mena"},
    {"name": "General esports transfer rumors (Google News bridge)", "url": "https://news.google.com/rss/search?q=esports+(rumor+OR+rumour+OR+%22reportedly+joining%22+OR+%22linked+with%22+OR+%22set+to+join%22)&hl=en&gl=US&ceid=US:en", "verified": False, "source_type": "leak"},

    # ============================================================
    # ADDED 2026-08-17 (spam-bug cleanup pass) — two more general
    # sources, both live-fetched and confirmed returning real, current
    # entries before being added here (not just copied from a
    # directory listing like most of the "verified": False block
    # above). Inven Global in particular is a strong add: very high
    # volume, very fast LCK/LPL match coverage (their English-language
    # feed), which nothing else in this file currently covers at that
    # depth.
    # ============================================================
    {"name": "Upcomer", "url": "https://upcomer.com/feed", "verified": True},
    {"name": "Inven Global (English)", "url": "https://www.invenglobal.com/feed/atom", "verified": True, "priority": "high"},
]

if __name__ == "__main__":
    import sys
    if "--test-sheep" in sys.argv:
        # Quick manual check: `python3 feeds.py --test-sheep`
        # Run this once after deploying, from an environment that can
        # actually reach sheepesports.com.
        name, entries, error = fetch_sheep_esports({"name": "Sheep Esports", "url": _SHEEP_URL})
        if error:
            print(f"FAILED: {error}")
        else:
            print(f"OK: {len(entries)} entries found\n")
            for e in entries[:10]:
                print(f"[{e.get('_sheep_scrape_source')}] {e.get('title')}")
                print(f"   {e.get('link')}")
                if e.get("published_parsed"):
                    print(f"   date: {_datetime(*e['published_parsed'][:6])}")
                print()
        sys.exit(0)

    print(f"Total feeds: {len(RSS_FEEDS)}")
    print(f"Currently working: {sum(1 for f in RSS_FEEDS if f.get('verified'))}")
    print(f"Currently failing (retried every run): {sum(1 for f in RSS_FEEDS if not f.get('verified'))}")
    print(f"Jordan-tagged: {sum(1 for f in RSS_FEEDS if f.get('region') == 'jordan')}")
    print(f"MENA-tagged: {sum(1 for f in RSS_FEEDS if f.get('region') == 'mena')}")
    print(f"Leak/rumor sources: {sum(1 for f in RSS_FEEDS if f.get('source_type') == 'leak')}")
    print(f"High priority: {sum(1 for f in RSS_FEEDS if f.get('priority') == 'high')}")

    names = [f['name'] for f in RSS_FEEDS]
    dupes = {n for n in names if names.count(n) > 1}
    print(f"Duplicate names: {dupes if dupes else 'none'}")
