"""
GGNewsAR Bot — Player Transfers module
======================================

Why this exists
---------------
The RSS phase only sees transfers that someone wrote an *article* about.
Most roster moves never become articles: bench moves, stand-ins, coach
changes, analysts, free-agent releases, academy promotions. Those are the
ones that break first and that nobody in Arabic is covering.

Liquipedia's "Player Transfers" pages are the primary ledger for every
roster move in every game — HLTV, VLR, Dot Esports and everyone else read
them too. This module reads them directly, so GGNewsAR gets the move at
source, minutes after it is logged, not hours after someone writes it up.

How it works (no API key needed)
--------------------------------
1. For each wiki, resolve the current month's transfer page
   (e.g. counterstrike/Player_Transfers/2026/July).
2. Cheap check: one batched API call per wiki for the latest revid.
   If the revid did not change since last run, skip the page entirely.
3. If it changed, fetch the page wikitext, extract every {{transfer}}
   template with a brace-balanced scanner (refs contain nested templates,
   so a plain regex is not safe here).
4. Hash each row (date + players + from + to + role). New hashes = new
   transfers. Send them.

Filtering (TRANSFERS_MODE)
--------------------------
  "priority" — only MENA / Arab orgs + Arab players. Lowest volume.
  "watched"  — MENA orgs + the tracked top international orgs and stars.
               (default: this is the GGNewsAR sweet spot)
  "all"      — literally every transfer on every watched wiki. This is a
               firehose: CS2 alone logs dozens a day. Use with a dedicated
               Discord channel only.

MENA rows are always sent regardless of mode, and are marked so you can
spot them instantly in Discord.
"""

# ============================================================
# Behaviour
# ============================================================
TRANSFERS_MODE = "watched"      # "priority" | "watched" | "all"
TRANSFERS_MAX_PER_RUN = 20      # cap so a mass roster shuffle can't flood
TRANSFERS_MIN_INTERVAL_MINUTES = 15


# ============================================================
# Wikis to track
# ============================================================
# key = Liquipedia wiki path, value = display name used in the Discord embed.
# Add or remove freely — an unsupported wiki just logs "no transfer page".
TRANSFER_WIKIS = {
    "counterstrike":   "CS2",
    "valorant":        "VALORANT",
    "dota2":           "Dota 2",
    "leagueoflegends": "League of Legends",
    "rainbowsix":      "Rainbow Six Siege",
    "rocketleague":    "Rocket League",
    "mobilelegends":   "Mobile Legends",
    "pubgmobile":      "PUBG Mobile",
    "honorofkings":    "Honor of Kings",
    "apexlegends":     "Apex Legends",
    "callofduty":      "Call of Duty",
    "overwatch":       "Overwatch",
    "easportsfc":      "EA Sports FC",
    "fighters":        "Fighting Games",
    "freefire":        "Free Fire",
    "fortnite":        "Fortnite",
    "starcraft2":      "StarCraft II",
}

# Page-title candidates, tried in order. Liquipedia is not consistent:
# most wikis use monthly pages, some only yearly, CrossFire-style wikis
# use quarters. {y} = year, {m} = month name, {q} = quarter (1st/2nd/...).
TRANSFER_PAGE_PATTERNS = [
    "Player_Transfers/{y}/{m}",
    "Player_Transfers/{y}/{q}_Quarter",
    "Player_Transfers/{y}",
    "Transfers/{y}/{m}",
    "Transfers/{y}",
]


# ============================================================
# Entities
# ============================================================
# MENA / Arab orgs — ALWAYS sent, in every mode, and flagged in Discord.
# Match is case-insensitive substring on both the old and new team fields.
PRIORITY_ORGS = [
    # Saudi
    "Team Falcons", "Falcons", "Twisted Minds", "Al-Ahli", "Al Ahli",
    "Al-Qadsiah", "Al Qadsiah", "Al-Hilal", "Al Hilal", "Al-Nassr", "Al Nassr",
    "Team Vitality Saudi", "Powerhouse", "Vega Squadron KSA",
    # UAE / Gulf
    "Geekay", "Geekay Esports", "FATE Esports", "FATE", "NASR Esports", "NASR",
    "YaLLa Esports", "YaLLa", "Emirates", "Qatar Esports", "Zain Esports",
    # Levant / Egypt / North Africa
    "Nigma Galaxy", "Nigma", "Anubis Gaming", "Anubis", "Vendetta Esports",
    "Sandstorm", "Villain Esports", "Onyx Esports", "Tunisia", "Morocco",
    # National teams (ENC / Asian Games rows use country names)
    "Saudi Arabia", "Jordan", "Egypt", "Iraq", "Kuwait", "Bahrain", "Oman",
    "Lebanon", "Palestine", "Syria", "Algeria", "Libya", "Sudan", "Yemen",
    "United Arab Emirates",
]

# Arab / MENA players worth flagging even when moving between non-Arab orgs.
PRIORITY_PLAYERS = [
    "GeneRaL", "Miracle-", "Miracle", "w1lla", "Anas", "Skiter", "Ahmed",
    "Yousef", "Mohammed", "Karim", "Ziad", "MoHaMeDoV",
]

# Top international orgs — sent in "watched" and "all" modes.
# Keep this roughly in sync with watchlist.py; it is the same universe of
# teams whose moves are worth an Arabic story.
TRACKED_ORGS = [
    # CS2
    "Vitality", "Team Spirit", "Spirit", "FaZe", "MOUZ", "G2", "Natus Vincere",
    "NAVI", "FURIA", "Astralis", "Team Liquid", "Liquid", "Eternal Fire",
    "The MongolZ", "MongolZ", "Virtus.pro", "Heroic", "3DMAX", "PARIVISION",
    "9z", "paiN", "Complexity", "NRG", "BC.Game", "Aurora", "GamerLegion",
    "Ninjas in Pyjamas", "TYLOO", "Lynn Vision", "B8", "Passion UA",
    # VALORANT
    "Sentinels", "Fnatic", "Paper Rex", "Team Heretics", "Heretics",
    "EDward Gaming", "DRX", "Gen.G", "100 Thieves", "LOUD", "Karmine Corp",
    "T1", "NRG", "G2 Esports", "Rex Regum Qeon", "BBL Esports", "BBL",
    "Wolves Esports", "Trace Esports", "Xi Lai Gaming", "Gentle Mates",
    # LoL
    "JD Gaming", "Bilibili Gaming", "Hanwha Life", "Dplus", "KT Rolster",
    "Weibo Gaming", "Top Esports", "Movistar KOI", "KOI", "Fnatic",
    # Dota 2
    "Gaimin Gladiators", "Tundra", "PSG Quest", "BetBoom", "Xtreme Gaming",
    "Team Falcons", "Yakult Brothers", "Nouns",
    # R6 / RL / mobile
    "DarkZero", "Shifters", "w7m", "Spacestation", "M80",
    "Karmine Corp", "Team BDS", "Dignitas", "Gen.G Mobil1",
    "ONIC", "RRQ", "EVOS", "Blacklist International", "Selangor Red Giants",
    "Team Vitality", "Alliance", "Cloud9", "Evil Geniuses", "Team Secret",
    "TSM", "Luminosity", "OpTic", "Team Falcons", "Twisted Minds",
]

# Star players — a bench/free-agent move by these is a story on its own,
# even if the destination is an org nobody tracks.
TRACKED_PLAYERS = [
    "s1mple", "ZywOo", "donk", "m0NESY", "NiKo", "device", "ropz", "frozen",
    "b1t", "Magisk", "kyousuke", "TeSeS", "karrigan", "sh1ro", "Ax1Le",
    "TenZ", "aspas", "Derke", "Chronicle", "Boaster", "f0rsakeN", "Jinggg",
    "yay", "Zekken", "Demon1", "something",
    "Faker", "Chovy", "Zeus", "Ruler", "Keria", "Caps", "Knight", "Bin",
    "Yatoro", "Collapse", "Miracle-", "N0tail", "Ame", "Nisha", "SumaiL",
    "ATK", "skc", "Cryn",
]


# ============================================================
# Parsing helpers (pure, unit-testable, no network)
# ============================================================
PARAM_ALIASES = {
    "date":    ["date", "date1"],
    "players": ["players", "player", "name", "name1", "player1", "p1"],
    "from":    ["team1", "from", "from1", "oldteam", "team_old", "old"],
    "to":      ["team2", "to", "to1", "newteam", "team_new", "new"],
    "role_from": ["pos1", "role1", "position1"],
    "role_to":   ["pos2", "role2", "position2"],
    "ref":     ["ref", "ref1", "source"],
}


def extract_transfer_templates(wikitext: str) -> list:
    """Brace-balanced scan for {{transfer ...}} blocks.

    A regex is NOT safe here: the |ref= field routinely contains nested
    templates ({{cite web|...}}), and a lazy regex stops at the first }}
    inside the ref, silently truncating the row.
    """
    out = []
    lowered = wikitext.lower()
    i = 0
    while True:
        start = lowered.find("{{transfer", i)
        if start == -1:
            break
        # Reject {{transferrow-lite}} style false hits only if followed by
        # a letter that makes it a *different* template name. Liquipedia
        # uses {{transfer}} and {{TransferRow}}; both are wanted.
        depth = 0
        j = start
        n = len(wikitext)
        while j < n - 1:
            pair = wikitext[j:j + 2]
            if pair == "{{":
                depth += 1
                j += 2
                continue
            if pair == "}}":
                depth -= 1
                j += 2
                if depth == 0:
                    out.append(wikitext[start:j])
                    break
                continue
            j += 1
        else:
            break
        i = j
    return out


def split_params(template: str) -> dict:
    """Split a template body on top-level pipes only (depth-0), so that
    nested templates and [[links|with pipes]] don't corrupt the fields."""
    body = template[2:-2]  # strip {{ }}
    parts = []
    buf = []
    depth_brace = 0
    depth_brack = 0
    k = 0
    n = len(body)
    while k < n:
        two = body[k:k + 2]
        if two == "{{":
            depth_brace += 1
            buf.append(two); k += 2; continue
        if two == "}}":
            depth_brace -= 1
            buf.append(two); k += 2; continue
        if two == "[[":
            depth_brack += 1
            buf.append(two); k += 2; continue
        if two == "]]":
            depth_brack -= 1
            buf.append(two); k += 2; continue
        ch = body[k]
        if ch == "|" and depth_brace == 0 and depth_brack == 0:
            parts.append("".join(buf))
            buf = []
            k += 1
            continue
        buf.append(ch)
        k += 1
    parts.append("".join(buf))

    params = {}
    for part in parts[1:]:  # parts[0] is the template name
        if "=" not in part:
            continue
        key, _, val = part.partition("=")
        params[key.strip().lower()] = val.strip()
    return params


def _first(params: dict, keys: list) -> str:
    for k in keys:
        v = params.get(k, "").strip()
        if v:
            return v
    return ""


def _clean(value: str) -> str:
    """Strip wiki markup down to plain text."""
    if not value:
        return ""
    import re as _re
    v = _re.sub(r"\{\{[^{}]*\}\}", "", value)          # nested templates
    v = _re.sub(r"\[\[([^\[\]|]*\|)?([^\[\]]*)\]\]", r"\2", v)  # [[a|b]] -> b
    v = _re.sub(r"<[^>]+>", "", v)                     # html/ref tags
    v = _re.sub(r"'{2,}", "", v)                       # bold/italic
    v = _re.sub(r"\s+", " ", v)
    return v.strip()


def parse_transfer(template: str) -> dict:
    """Turn one {{transfer}} template into a normalized dict."""
    p = split_params(template)
    row = {
        "date":      _clean(_first(p, PARAM_ALIASES["date"])),
        "players":   _clean(_first(p, PARAM_ALIASES["players"])),
        "from":      _clean(_first(p, PARAM_ALIASES["from"])),
        "to":        _clean(_first(p, PARAM_ALIASES["to"])),
        "role_from": _clean(_first(p, PARAM_ALIASES["role_from"])),
        "role_to":   _clean(_first(p, PARAM_ALIASES["role_to"])),
        "ref":       _first(p, PARAM_ALIASES["ref"]),
    }
    # Some wikis number players: name1..name5 / player1..player5
    if not row["players"]:
        names = [_clean(p[k]) for k in sorted(p) if k.startswith(("name", "player", "p")) and p[k]]
        row["players"] = ", ".join(n for n in names if n)
    return row


def _hit(haystack: str, needles: list) -> str:
    """Return the first needle found (case-insensitive substring), else ''."""
    h = (haystack or "").lower()
    for n in needles:
        if n.lower() in h:
            return n
    return ""


def classify(row: dict) -> tuple:
    """Decide whether to send this row, and how to tag it.

    Returns (should_send: bool, tag: str). tag is "" for a normal row,
    "MENA" for an Arab org/player row.
    """
    blob = " | ".join([row.get("from", ""), row.get("to", ""), row.get("players", "")])
    teams = " | ".join([row.get("from", ""), row.get("to", "")])
    players = row.get("players", "")

    if _hit(teams, PRIORITY_ORGS) or _hit(players, PRIORITY_PLAYERS):
        return True, "MENA"

    if TRANSFERS_MODE == "all":
        return True, ""

    if TRANSFERS_MODE == "watched":
        if _hit(teams, TRACKED_ORGS) or _hit(players, TRACKED_PLAYERS):
            return True, ""
        return False, ""

    return False, ""  # "priority" mode: MENA only


def row_key(wiki: str, row: dict) -> str:
    """Stable identity for a transfer, used for dedup across runs."""
    import hashlib
    raw = "|".join([
        wiki,
        row.get("date", ""),
        row.get("players", ""),
        row.get("from", ""),
        row.get("to", ""),
        row.get("role_to", ""),
    ]).lower()
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def format_headline(game: str, row: dict) -> str:
    """Short, scannable Discord title."""
    players = row.get("players") or "?"
    old = row.get("from") or "Free Agent"
    new = row.get("to") or "Free Agent"
    if row.get("role_to"):
        new = f"{new} ({row['role_to']})"
    return f"{game}: {players} — {old} → {new}"


if __name__ == "__main__":
    sample = """
{{transfer
|players=NiKo
|team1=G2 Esports |pos1= |team2=Team Falcons |pos2=
|date=2026-07-14
|ref={{cite web|url=https://www.hltv.org/news/12345|title=Official: NiKo joins Falcons}}
}}
{{transfer|players=some_guy|team1=|team2=Vitality|date=2026-07-14|pos2=Coach}}
{{transfer|players=nobody|team1=Tiny Org|team2=|date=2026-07-14}}
"""
    for t in extract_transfer_templates(sample):
        r = parse_transfer(t)
        send, tag = classify(r)
        print(f"send={send!s:5s} tag={tag:5s} {format_headline('CS2', r)}")
