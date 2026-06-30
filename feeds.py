"""
GGNewsAR Bot — RSS Feed Configuration
105 English-language esports sources. No Arabic sources by design.

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
"""

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
    {"name": "GamingPH", "url": "https://gamingph.com/feed", "verified": True},
    {"name": "RealSport101", "url": "https://realsport101.com/feed.xml", "verified": True},
    {"name": "TechRadar Gaming", "url": "https://www.techradar.com/feeds/tag/gaming", "verified": True},
    {"name": "Mobile Gaming Hub", "url": "https://mobilegaminghub.com/feed", "verified": True},
    {"name": "PC Games N Esports", "url": "https://www.pcgamesn.com/feed", "verified": True},

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
    {"name": "Fragster", "url": "https://fragster.com/feed", "verified": False},
    {"name": "Hotspawn", "url": "https://www.hotspawn.com/feed", "verified": False},
    {"name": "G2G News Esports", "url": "https://g2g.news/feed", "verified": False},
    {"name": "GamingOnPhone", "url": "https://gamingonphone.com/feed", "verified": False},
    {"name": "ONE Esports", "url": "https://www.oneesports.gg/feed", "verified": False},
    {"name": "Way to Smurf", "url": "https://www.waytosmurf.com/feed", "verified": False},
    {"name": "UKCSGO", "url": "https://ukcsgo.com/feed", "verified": False},
    {"name": "CSGO2ASIA", "url": "https://csgo2asia.com/feed", "verified": False},
    {"name": "Esports Talk CS", "url": "https://esportstalk.com/blog/csgo/feed", "verified": False},
    {"name": "Esports Talk Valorant", "url": "https://esportstalk.com/blog/valorant/feed", "verified": False},
    {"name": "Esports.net Valorant", "url": "https://www.esports.net/news/valorant/feed", "verified": False},
    {"name": "Fragster Valorant", "url": "https://fragster.com/valorant/feed", "verified": False},
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
    {"name": "Fragster Overwatch", "url": "https://fragster.com/overwatch/feed", "verified": False},
    {"name": "Esports Talk Overwatch", "url": "https://esportstalk.com/blog/overwatch/feed", "verified": False},
    {"name": "DBLTap Overwatch", "url": "https://www.dbltap.com/leagues/overwatch/feed", "verified": False},
    {"name": "Hotspawn Overwatch", "url": "https://www.hotspawn.com/overwatch/news/feed", "verified": False},
    {"name": "Esports Talk CoD", "url": "https://esportstalk.com/blog/call-of-duty/feed", "verified": False},
    {"name": "ONE Esports CoD", "url": "https://www.oneesports.gg/call-of-duty/feed", "verified": False},
    {"name": "ONE Esports Apex", "url": "https://www.oneesports.gg/apex-legends/feed", "verified": False},
    {"name": "Dot Esports PUBG", "url": "https://dotesports.com/pubg/feed", "verified": False},
    {"name": "Esports Talk PUBG Mobile", "url": "https://esportstalk.com/news/pubg-mobile/feed", "verified": False},
    {"name": "DBLTap PUBG", "url": "https://www.dbltap.com/leagues/pubg/feed", "verified": False},
    {"name": "GamingOnPhone News", "url": "https://gamingonphone.com/category/news/feed", "verified": False},
    {"name": "Esports.net Mobile Games", "url": "https://www.esports.net/news/mobile-games/feed", "verified": False},
    {"name": "RLRSS", "url": "https://rlrss.qrivi.dev/feed", "verified": False},
    {"name": "EventHubs", "url": "https://www.eventhubs.com/feed/", "verified": False},
    {"name": "AFK Gaming Alt", "url": "https://afkgaming.com/feed", "verified": False},
    {"name": "InsideSport Esports", "url": "https://insidesport.in/topic/esports/feed", "verified": False},
    {"name": "India Today Gaming", "url": "https://www.indiatodaygaming.com/feed", "verified": False},
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
]


if __name__ == "__main__":
    print(f"Total feeds: {len(RSS_FEEDS)}")
    print(f"Currently working: {sum(1 for f in RSS_FEEDS if f.get('verified'))}")
    print(f"Currently failing (retried every run): {sum(1 for f in RSS_FEEDS if not f.get('verified'))}")

