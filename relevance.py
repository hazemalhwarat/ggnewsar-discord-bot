"""
GGNewsAR Bot — Esports relevance classifier (keyword-based, no AI/API calls).

Goal: decide whether a raw news/post item is actually about COMPETITIVE
esports (games, teams, players, tournaments, orgs, business/sponsorship
deals in the esports space) — not just "gaming" in general, and not a
story-driven/single-player game, hardware deal, or unrelated article that
happened to match a broad search query.

This module is intentionally free of any external API/model calls: it's
pure string matching, so it's free, instant, and has zero rate limits or
extra secrets to manage. It will never be as precise as an LLM classifier,
but it removes the two failure modes Hazem flagged:
  1. Broad Google-News "site:" / keyword-search bridges pulling in
     articles that only tangentially mention a country + the word
     "esports" without actually being esports news.
  2. Non-competitive game content (story games, walkthroughs, hardware
     deals) leaking in through general "gaming" queries.

Usage (see bot.py):
    from relevance import is_relevant_esports
    if not trusted_source and not is_relevant_esports(title + " " + summary):
        skip this item
"""

import re

# ------------------------------------------------------------
# 1) Competitive esports titles. Matching one of these is a strong signal
#    the article/post is *about a specific competitive game*, but on its
#    own isn't proof it's about the COMPETITIVE side of that game (e.g. a
#    "Valorant skins leak" article also matches). Combine with context.
# ------------------------------------------------------------
COMPETITIVE_GAME_TERMS = [
    "counter-strike", "counter strike", "cs2", "csgo", "cs:go",
    "valorant", "league of legends", "lol esports", "wild rift",
    "dota 2", "dota2", "teamfight tactics", "tft",
    "overwatch", "rocket league", "rainbow six siege", "r6 siege",
    "siege esports", "pubg", "pubg mobile", "bgmi",
    "call of duty league", "cdl", "warzone esports",
    "mobile legends", "mlbb", "free fire", "honor of kings", "arena of valor",
    "clash royale", "brawl stars esports",
    "fortnite", "fncs", "apex legends", "algs",
    "fifae", "fc pro", "efootball", "efootball esports",
    "street fighter", "tekken", "evo championship", "guilty gear esports",
    "smash bros", "super smash", "melee esports",
    "starcraft", "age of empires", "hearthstone esports",
    "chess.com", "chess esports",
    "king of glory",
]

# ------------------------------------------------------------
# 2) Strong context: the word itself is basically unambiguous. If either
#    of these appears anywhere, the item is accepted on its own — no need
#    to also match a game name.
# ------------------------------------------------------------
STRONG_CONTEXT_TERMS = [
    "esports", "e-sports", "esport ", "esports.", "esports,", "esports\n",
    "الرياضات الإلكترونية", "رياضة إلكترونية", "الرياضة الإلكترونية",
    "رياضات إلكترونية", "e-sport",
]

# ------------------------------------------------------------
# 3) Weak context: common in esports coverage, but also common in
#    traditional sports / unrelated coverage. Only counts when paired
#    with a competitive-game term OR a known org/tournament name.
# ------------------------------------------------------------
WEAK_CONTEXT_TERMS = [
    "tournament", "championship", "grand final", "playoffs", "qualifier",
    "qualifiers", "roster", "lineup", "transfer window", "signs", "signed",
    "benched", "coach", "captain", "prize pool", "lan event", "bracket",
    "group stage", "best of", "major", "world championship", "world finals",
    "world cup", "worlds", "msi", "regular season", "franchise slot",
    "بطولة", "دوري", "تصفيات", "روستر", "تشكيلة", "تعاقد", "استغناء",
    "مدرب", "كابتن", "نهائي", "كأس العالم", "منتخب", "لاعب محترف",
]

# ------------------------------------------------------------
# 4) Known orgs / tournaments / federations / brands. Any match here is
#    accepted immediately — these names are inherently esports-specific,
#    no extra context needed. Keep this list growing over time.
# ------------------------------------------------------------
KNOWN_ENTITIES = [
    # Arab / MENA teams & orgs
    "team falcons", "falcons esports", "twisted minds", "nigma galaxy",
    "geekay esports", "anubis gaming", "nasr esports", "team vision",
    "psg esports", "fate esports", "al-ahli esports", "al ahli esports",
    # Arab federations / bodies
    "saudi esports federation", "الاتحاد السعودي للرياضة الإلكترونية",
    "egyptian esports federation", "الاتحاد المصري للرياضات الإلكترونية",
    "jordan esports federation", "jef", "الاتحاد الأردني للرياضة الإلكترونية",
    "uae esports federation", "اتحاد الإمارات للرياضات الإلكترونية",
    "general authority for competitive and electronic sports",
    "savvy games group", "esports world cup foundation", "ewcf",
    "gamers8", "qiddiya", "neom esports", "riyadh season esports",
    "the arc", "mahara academy",
    # Global tournaments / leagues / orgs
    "esports world cup", " ewc ", "iem ", "intel extreme masters",
    "esl ", "blast premier", "pgl ", "vct ", "lck", "lpl", "lec", "lcs",
    "six invitational", "champions tour", "esports nations cup",
    "rlcs", "the international dota", "worlds 2026", "worlds 2027",
]

# ------------------------------------------------------------
# 5) Hard exclusions: non-competitive gaming content that commonly leaks
#    in through broad "gaming"/country-name searches. If one of these
#    matches AND no known entity is present, the item is rejected outright
#    regardless of anything else.
# ------------------------------------------------------------
HARD_EXCLUDE_TERMS = [
    # story-driven / single-player signals
    "walkthrough", "story mode", "campaign mode", "visual novel",
    "dating sim", "otome game", "choose your own adventure",
    "single-player review", "single player review", "narrative game",
    "walking simulator", "point-and-click adventure",
    "لعبة قصصية", "وضع القصة", "قصة اللعبة",
    # hardware / shopping deals, not competitive news
    "gaming laptop deal", "best gaming laptop", "gaming chair deal",
    "gaming mouse deal", "gaming keyboard deal", "graphics card deal",
    "black friday gaming", "prime day gaming", "gaming monitor review",
    "gaming pc build",
    # unrelated media
    "movie adaptation", "film adaptation", "tv series review",
    "anime episode",
]

# Pre-lowered for speed
_COMPETITIVE_GAME_TERMS = [t.lower() for t in COMPETITIVE_GAME_TERMS]
_STRONG_CONTEXT_TERMS = [t.lower() for t in STRONG_CONTEXT_TERMS]
_WEAK_CONTEXT_TERMS = [t.lower() for t in WEAK_CONTEXT_TERMS]
_KNOWN_ENTITIES = [t.lower() for t in KNOWN_ENTITIES]
_HARD_EXCLUDE_TERMS = [t.lower() for t in HARD_EXCLUDE_TERMS]

_WS_RE = re.compile(r"\s+")


def _norm(text: str) -> str:
    t = (text or "").lower()
    t = _WS_RE.sub(" ", t)
    return f" {t} "  # pad so " ewc " style tokens can match at edges


def is_relevant_esports(text: str) -> bool:
    """True if the given title+summary text looks like genuine
    competitive-esports content (game/team/tournament/business news),
    False if it looks like noise, a non-competitive game, or unrelated
    content that only matched a broad search query by coincidence.

    Call this ONLY for untrusted/broad sources (Google News keyword
    search bridges, Reddit posts). Dedicated esports-site RSS feeds and
    official team/tournament X accounts should bypass this entirely —
    see the `is_search_bridge` heuristic in bot.py.
    """
    t = _norm(text)

    has_entity = any(e in t for e in _KNOWN_ENTITIES)
    if has_entity:
        return True

    has_hard_exclude = any(x in t for x in _HARD_EXCLUDE_TERMS)
    if has_hard_exclude:
        return False

    has_strong_context = any(s in t for s in _STRONG_CONTEXT_TERMS)
    if has_strong_context:
        return True

    has_game = any(g in t for g in _COMPETITIVE_GAME_TERMS)
    has_weak_context = any(w in t for w in _WEAK_CONTEXT_TERMS)
    if has_game and has_weak_context:
        return True

    return False


def is_hard_excluded(text: str) -> bool:
    """True if the text matches a hard-exclusion phrase (non-competitive
    game, hardware deal, unrelated media) AND has no known entity to
    override it. Exposed separately from is_relevant_esports so callers
    (bot.py) can tell "definitely not esports" apart from "just
    ambiguous, no signal either way" — the AI second-opinion check
    (ai_classifier.py) is only worth spending quota on the ambiguous
    case, never the definitely-excluded one.
    """
    t = _norm(text)
    if any(e in t for e in _KNOWN_ENTITIES):
        return False
    return any(x in t for x in _HARD_EXCLUDE_TERMS)


def classify_keyword(text: str) -> str:
    """Three-way keyword verdict: "accept" (send it, no AI needed),
    "reject_hard" (definitely not esports, no AI needed), or "ambiguous"
    (no signal either way — worth a cheap AI second opinion before
    rejecting, since this is exactly the bucket where a real story about
    an unlisted team/player/tournament falls through the keyword net).
    """
    if is_relevant_esports(text):
        return "accept"
    if is_hard_excluded(text):
        return "reject_hard"
    return "ambiguous"


# ------------------------------------------------------------
# 6) GAME-CONTENT noise — added 2026-08-24 per Hazem's rule:
#    GGNewsAR is an ESPORTS wire, not a games wire. Content about the
#    game itself (patch notes, meta/balance changes, new skins/agents,
#    battle passes, release dates, reviews, guides, tier lists, cosplay,
#    fan art) is NOT wanted — UNLESS the item also ties to the
#    competitive/pro scene, a tournament, an org, or a person's move
#    (incl. streamers/creators signing with an org). Those exceptions are
#    exactly the KNOWN_ENTITIES / WEAK_CONTEXT / business-signal lists.
#
#    Design notes (kept deliberately conservative per "when borderline,
#    lean toward coverage"):
#      - The term list is only UNAMBIGUOUS game-content wording. Words
#        that overlap with competitive news (leak, season, new map,
#        collab/crossover, bare "update"/"event") are intentionally
#        EXCLUDED so roster leaks, competitive seasons, map-pool changes
#        and brand collabs are never caught here.
#      - The gate fires ONLY when a game-content term is present AND no
#        pro/competitive/business/person-org signal is present. So a
#        normal esports story (which always carries an org, tournament,
#        roster, transfer or sponsorship signal) is never dropped.
# ------------------------------------------------------------
GAME_CONTENT_TERMS = [
    "patch notes", "patch note", " patch ", "hotfix", "balance change",
    "balance patch", "buff", "nerf", "new agent", "new champion",
    "new hero", "new operator", "new legend", "new skin", "skins",
    "skin bundle", "cosmetic", "battle pass", "battlepass", "season pass",
    "gameplay trailer", "launch trailer", "reveal trailer",
    "cinematic trailer", "release date", "launch date", "early access",
    "open beta", "closed beta", "playtest", "game review", "tier list",
    "best settings", "best sensitivity", "best loadout", "best build",
    "settings for", "max fps", "fps boost", "boost fps", "graphics settings",
    "config guide", "sensitivity settings", "beginner guide",
    "beginner's guide", "how to unlock", "how to get", "how to complete",
    "walkthrough", "dlc", "expansion pack", "new game mode",
    "limited time mode", "microtransaction", "wallpaper", "fan art",
    "fanart", "cosplay",
    # Arabic
    "باتش", "تحديث اللعبة", "تغييرات التوازن", "بطل جديد", "عميل جديد",
    "شخصية جديدة", "سكن", "سكنات", "بطاقة الموسم", "بطاقة القتال",
    "عرض الجيمبلاي", "العرض الدعائي", "موعد الإصدار", "تاريخ الإصدار",
    "النسخة التجريبية", "مراجعة اللعبة", "دليل المبتدئين", "أفضل إعدادات",
    "وضع لعب جديد", "توسعة", "حزمة سكنات", "خلفية", "كوسبلاي",
]

# Extra "keep" signals beyond KNOWN_ENTITIES + WEAK_CONTEXT: business
# (sponsorship/investment) and person-org ties (incl. streamers signing
# with an org — Hazem: "anyone signed with an esports org is in scope").
EXTRA_KEEP_SIGNAL = [
    "sponsor", "sponsorship", "partnership", "partner with", "brand ambassador",
    "investment", "acquisition", "acquires", "signs with", "joins", "joining",
    "parts ways", "academy", "competitive", "scrim", "pro player", "pro scene",
    "رعاية", "رعايات", "شراكة", "شراكات", "راعي", "استثمار", "صفقة", "سفير",
    "انضم", "انضمام", "أكاديمية", "احتراف", "تنافسي",
]

_GAME_CONTENT_TERMS = [t.lower() for t in GAME_CONTENT_TERMS]
_KEEP_SIGNAL = _KNOWN_ENTITIES + _WEAK_CONTEXT_TERMS + [t.lower() for t in EXTRA_KEEP_SIGNAL]


def is_game_content_noise(text: str) -> bool:
    """True if the item is about the GAME itself (patch/meta/skins/launch/
    review/guide/cosplay) with NO tie to the competitive scene, a
    tournament, an org, or a person's org move. Such items are outside
    GGNewsAR's esports-only scope and should be dropped.

    Returns False (i.e. keep) for anything that isn't game content, and
    for game content that DOES carry a pro/competitive/business/person-org
    signal (e.g. a patch that shifts the VCT meta, an agent banned from
    pro play, a streamer signing with an org).
    """
    t = _norm(text)
    if not any(g in t for g in _GAME_CONTENT_TERMS):
        return False  # not game content — never our business to drop here
    if any(k in t for k in _KEEP_SIGNAL):
        return False  # game content, but tied to the pro/esports scene — keep
    return True       # pure game content, no esports tie — drop


if __name__ == "__main__":
    # Quick manual sanity checks
    tests = [
        ("Falcons win the CS2 Major in Paris", True),
        ("Team Falcons announce new Valorant roster", True),
        ("Best gaming laptops to buy this Black Friday", False),
        ("Bahrain launches new visual novel dating sim on Steam", False),
        ("EWC 2026: Team Vision crowned TFT champions", True),
        ("Qatar to host cultural heritage festival", False),
        ("Saudi Esports Federation signs new sponsorship with stc", True),
        ("Random article that never mentions esports at all", False),
    ]
    for text, expected in tests:
        got = is_relevant_esports(text)
        status = "OK" if got == expected else "FAIL"
        print(f"[{status}] expected={expected} got={got} :: {text}")
