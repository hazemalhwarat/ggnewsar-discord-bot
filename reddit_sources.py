"""
GGNewsAR Bot — Reddit sourcing (public JSON endpoints, no OAuth needed).

Reddit's read-only JSON API (https://www.reddit.com/r/<sub>/new.json) works
without any app registration or API key as long as a distinct User-Agent is
sent — anonymous default UAs get blocked/rate-limited, a custom one is
generally fine for this call volume (once per ~15 min across a couple
dozen subreddits).

Every post pulled from here still goes through the SAME relevance filter
as the broad Google News bridges in feeds.py (see relevance.py) before
bot.py ever sends it — subreddits are community feeds, not curated esports
sources, so nothing here is ever "trusted" by default.

To add/remove a subreddit: just edit SUBREDDITS below. "limit" controls how
many of the newest posts are checked per pass (freshness + dedup in
bot.py still filter out anything already seen/old).
"""

import requests

USER_AGENT = "GGNewsARBot/1.0 (by /u/hazemalhwarat; contact: ggnewsar)"

REDDIT_FETCH_TIMEOUT_SECONDS = 10

# Junk flairs to always skip regardless of keyword match — these are
# community-fun content, not news, even when they mention a team/game.
JUNK_FLAIRS = {
    "meme", "memes", "fluff", "fan art", "fanart", "highlight",
    "highlights", "achievement", "humor", "humour", "clip",
}

SUBREDDITS = [
    {"name": "esports", "limit": 25},
    {"name": "GlobalOffensive", "limit": 25},
    {"name": "counterstrike", "limit": 20},
    {"name": "VALORANT", "limit": 25},
    {"name": "leagueoflegends", "limit": 20},
    {"name": "DotA2", "limit": 20},
    {"name": "CompetitiveOverwatch", "limit": 20},
    {"name": "RocketLeagueEsports", "limit": 15},
    {"name": "Rainbow6", "limit": 15},
    {"name": "PUBATTLEGROUNDS", "limit": 15},
    {"name": "MobileLegendsGame", "limit": 15},
    {"name": "TeamfightTactics", "limit": 15},
    {"name": "apexlegends", "limit": 15},
    {"name": "smashbros", "limit": 15},
]


def fetch_subreddit(sub_info: dict):
    """Fetch the newest posts from one subreddit. Never raises — returns
    (name, posts_or_None, error_or_None), same contract as
    feeds.fetch_one_feed so bot.py can treat both sources uniformly.

    Each post dict has: title, link, summary, image, published_ts
    (unix seconds, may be None), score, flair.
    """
    name = sub_info["name"]
    limit = sub_info.get("limit", 20)
    url = f"https://www.reddit.com/r/{name}/new.json?limit={limit}&raw_json=1"
    try:
        resp = requests.get(
            url,
            timeout=REDDIT_FETCH_TIMEOUT_SECONDS,
            headers={"User-Agent": USER_AGENT},
        )
        resp.raise_for_status()
        data = resp.json()
        children = data.get("data", {}).get("children", [])
        posts = []
        for child in children:
            d = child.get("data", {})
            if d.get("stickied") or d.get("over_18"):
                continue
            flair = (d.get("link_flair_text") or "").strip().lower()
            if flair in JUNK_FLAIRS:
                continue
            score = d.get("score", 0) or 0
            if score < 1:
                continue
            title = (d.get("title") or "").strip()
            permalink = d.get("permalink") or ""
            link = f"https://www.reddit.com{permalink}" if permalink else (d.get("url") or "")
            summary = (d.get("selftext") or "")[:500]
            image = ""
            preview = d.get("preview", {}).get("images", [])
            if preview:
                image = preview[0].get("source", {}).get("url", "")
            posts.append({
                "title": title,
                "link": link,
                "summary": summary,
                "image": image,
                "published_ts": d.get("created_utc"),
                "score": score,
                "flair": flair,
            })
        return name, posts, None
    except Exception as e:
        return name, None, str(e)


if __name__ == "__main__":
    for sub in SUBREDDITS[:2]:
        n, posts, err = fetch_subreddit(sub)
        print(n, "->", err if err else f"{len(posts)} posts")
