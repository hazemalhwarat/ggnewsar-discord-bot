"""
GGNewsAR Bot — Liquipedia Watchlist
Pages to monitor for changes. Edit this file to add or remove pages.

Rebalanced 2026-06-29: CS2 and VALORANT previously dominated this list
because they had the most pages, which meant they generated proportionally
more Liquipedia messages too. Other titles (R6, RL, MLBB, PUBGM, HoK) are
now expanded to comparable page counts, and two wikis that were entirely
missing (Fighting Games, EA Sports FC) have been added.

Notes:
- Use underscores in page titles, exactly as in the Liquipedia URL
- For example, "Team Falcons" → "Team_Falcons"
- Per game, you can have up to ~150 pages before each run becomes slow
"""

WATCHLIST = {
    # === Counter Strike 2 ===
    "counterstrike": [
        # Arab teams (priority)
        "Team_Falcons",
        "Twisted_Minds",
        "Nigma_Galaxy",
        # Top international teams
        "Vitality",
        "Spirit",
        "FaZe",
        "MOUZ",
        "G2_Esports",
        "Natus_Vincere",
        "FURIA",
        "Astralis",
        "Team_Liquid",
        "Eternal_Fire",
        "TheMongolz",
        # Star players
        "S1mple",
        "ZywOo",
        "Donk",
        "M0NESY",
        "NiKo",
        "Magisk",
        "B1t",
        "Ropz",
        "Frozen",
        # Active tournaments and seasons
        "IEM_Cologne_2026",
        "BLAST_Premier/Spring/2026",
        "Esports_World_Cup/2026/Counter-Strike_2",
        "Esports_Nations_Cup/2026/Counter-Strike_2",
    ],

    # === VALORANT ===
    "valorant": [
        # Arab teams
        "Team_Falcons",
        "Twisted_Minds",
        # Top international
        "Sentinels",
        "Fnatic",
        "Paper_Rex",
        "Team_Heretics",
        "Team_Liquid",
        "EDward_Gaming",
        "DRX",
        "Gen.G",
        "100_Thieves",
        # Tournaments
        "Esports_World_Cup/2026",
        "Esports_Nations_Cup/2026",
        "VCT/2026/Masters",
        "VCT/2026/Champions",
    ],

    # === League of Legends ===
    "leagueoflegends": [
        # Top teams
        "T1",
        "Gen.G",
        "JD_Gaming",
        "Bilibili_Gaming",
        "G2_Esports",
        "Fnatic",
        "Hanwha_Life_Esports",
        # Tournaments
        "2026_Season_World_Championship",
        "LCK/2026",
        "LEC/2026",
        "LPL/2026",
        "Esports_World_Cup/2026/League_of_Legends",
    ],

    # === Dota 2 ===
    "dota2": [
        # Arab teams
        "Nigma_Galaxy",
        "Team_Falcons",
        # Top teams
        "Team_Spirit",
        "Gaimin_Gladiators",
        "Team_Liquid",
        "PSG_Quest",
        "Tundra_Esports",
        # Tournaments
        "The_International/2026",
        "Esports_World_Cup/2026/Dota_2",
    ],

    # === Rainbow Six Siege (expanded) ===
    "rainbowsix": [
        # Arab teams
        "Team_Falcons",
        "Twisted_Minds",
        # Top international teams (from live Liquipedia standings)
        "DarkZero",
        "Ninjas_in_Pyjamas",
        "Team_Liquid",
        "Shifters",
        "G2_Esports",
        "w7m_esports",
        "Spacestation_Gaming",
        "FaZe_Clan",
        "Wolves_Esports",
        "Virtus.pro",
        # Tournaments
        "Six_Invitational/2026",
        "Esports_World_Cup/2026",
        "Esports_Nations_Cup/2026",
        "BLAST_R6_Major/2026/Salt_Lake_City",
    ],

    # === Rocket League (expanded) ===
    "rocketleague": [
        # Arab teams
        "Team_Falcons",
        "Twisted_Minds",
        # Top international teams (from live Liquipedia standings)
        "Vitality",
        "NRG",
        "Karmine_Corp",
        "Spacestation_Gaming",
        "Dignitas",
        "Gen.G_Mobil1_Racing",
        "Gentle_Mates",
        "Team_BDS",
        "FURIA",
        # Tournaments
        "RLCS_2026",
        "Esports_World_Cup/2026",
        "FIFAe_World_Cup/2026",
    ],

    # === Mobile Legends (expanded) ===
    "mobilelegends": [
        "ONIC_Esports",
        "RRQ_Hoshi",
        "EVOS_Glory",
        "Selangor_Red_Giants",
        "Blacklist_International",
        "Echo",
        "TLID",
        "Falcons_Esports",
        "MPL/Indonesia/2026",
        "MPL/Philippines/2026",
        "MSC/2026",
        "M_Series/2026",
        "Esports_World_Cup/2026/Mobile_Legends:_Bang_Bang",
    ],

    # === Honor of Kings (expanded) ===
    "honorofkings": [
        "King_Pro_League/2026",
        "Honor_of_Kings_World_Cup/2026",
        "Esports_World_Cup/2026",
    ],

    # === PUBG Mobile (expanded) ===
    "pubgmobile": [
        # Arab teams
        "Team_Falcons",
        # Major tournaments
        "PMGC/2026",
        "PMPL/2026",
        "PMCO/2026",
        "Esports_World_Cup/2026",
        "PUBG_Mobile_Asian_Games_2026",
    ],

    # === Fighting Games (new) ===
    "fighters": [
        # Top players across the major titles (Tekken, Street Fighter)
        "Tekken",
        "Street_Fighter_6",
        "Tekken_World_Tour/2026",
        "Capcom_Cup/12",
        "Esports_World_Cup/2026/T8",
        "Esports_World_Cup/2026/SF6",
        "EVO/2026",
    ],

    # === EA Sports FC (new) ===
    "easportsfc": [
        "FC_Pro_26_World_Championship",
        "EChampions_League/2026",
        "Esports_World_Cup/2026",
    ],
}


def total_pages() -> int:
    """Total pages being watched across all wikis."""
    return sum(len(pages) for pages in WATCHLIST.values())


def all_wikis() -> list[str]:
    return list(WATCHLIST.keys())


if __name__ == "__main__":
    print(f"Total pages: {total_pages()}")
    for wiki, pages in WATCHLIST.items():
        print(f"  {wiki:18s} {len(pages)} pages")

