#!/usr/bin/env python3
import os
import re
import json
import time
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

ESPN_SITE_API = "https://site.api.espn.com/apis/site/v2"

DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# -----------------------------
# THPORTH leagues (ESPN scoreboard paths)
# -----------------------------
# Notes:
# - Soccer uses ESPN league codes like eng.1 (EPL) (commonly used in ESPN endpoints). :contentReference[oaicite:8]{index=8}
# - Not every league you care about will have a reliable ESPN recap article; we still store gamecast/recap URL if present.
LEAGUES: Dict[str, Dict[str, Any]] = {
    # Big 4 US
    "nba":  {"sport_path": "basketball/nba", "out_dir": "recaps/nba",  "yt_official": {"channelId": "UCWJ2lWNubArHWmf3FIHbfcQ"}},  # :contentReference[oaicite:9]{index=9}
    "nhl":  {"sport_path": "hockey/nhl",     "out_dir": "recaps/nhl",  "yt_official": {"channelId": "UCqFMzb-4AUf6WAIbl132QKA"}},  # :contentReference[oaicite:10]{index=10}
    "mlb":  {"sport_path": "baseball/mlb",   "out_dir": "recaps/mlb",  "yt_official": {"channelId": "UCoLrcjPV5PbUrUyXq5mjc_A"}},   # :contentReference[oaicite:11]{index=11}
    "nfl":  {"sport_path": "football/nfl",   "out_dir": "recaps/nfl",  "yt_official": {"channelId": "UCDVYQ4Zhbm3S2dlz7P1GBDg"}},   # :contentReference[oaicite:12]{index=12}

    # Basketball
    "wnba": {"sport_path": "basketball/wnba", "out_dir": "recaps/wnba", "yt_official": {"channelQuery": "WNBA"}},

    # NCAA (ESPN has these scoreboards; recaps may be inconsistent)
    "ncaam":  {"sport_path": "basketball/mens-college-basketball",   "out_dir": "recaps/ncaam",  "yt_official": None},
    "ncaawb": {"sport_path": "basketball/womens-college-basketball", "out_dir": "recaps/ncaawb", "yt_official": None},
    "ncaaf":  {"sport_path": "football/college-football",           "out_dir": "recaps/ncaaf",  "yt_official": None},

    # Soccer / UEFA / MLS (highlights rights vary; we still attempt official channels)
    "epl":   {"sport_path": "soccer/eng.1",           "out_dir": "recaps/epl",   "yt_official": {"channelQuery": "Premier League"}},  # :contentReference[oaicite:13]{index=13}
    "mls":   {"sport_path": "soccer/usa.1",           "out_dir": "recaps/mls",   "yt_official": {"channelId": "UCSZbXT5TLLW_i-5W8FZpFsg"}},  # :contentReference[oaicite:14]{index=14}
    "laliga":{"sport_path": "soccer/esp.1",           "out_dir": "recaps/laliga","yt_official": {"channelQuery": "LALIGA"}},  # resolve via API
    "seriea":{"sport_path": "soccer/ita.1",           "out_dir": "recaps/seriea","yt_official": {"channelQuery": "Serie A"}},  # resolve via API
    "bund":  {"sport_path": "soccer/ger.1",           "out_dir": "recaps/bund",  "yt_official": {"channelQuery": "Bundesliga"}},  # resolve via API
    "ligue1":{"sport_path": "soccer/fra.1",           "out_dir": "recaps/ligue1","yt_official": {"channelQuery": "Ligue 1"}},  # resolve via API
    "ucl":   {"sport_path": "soccer/uefa.champions",  "out_dir": "recaps/ucl",   "yt_official": {"channelId": "UCyGa1YEx9ST66rYrJTGIKOw"}},  # UEFA :contentReference[oaicite:15]{index=15}
    "uel":   {"sport_path": "soccer/uefa.europa",     "out_dir": "recaps/uel",   "yt_official": {"channelId": "UCyGa1YEx9ST66rYrJTGIKOw"}},  # UEFA :contentReference[oaicite:16]{index=16}
    "uecl":  {"sport_path": "soccer/uefa.europa.conf","out_dir": "recaps/uecl",  "yt_official": {"channelId": "UCyGa1YEx9ST66rYrJTGIKOw"}},  # UEFA :contentReference[oaicite:17]{index=17}

    # Motorsports (ESPN coverage varies; highlights are easy/official)
    "f1":     {"sport_path": "racing/f1",       "out_dir": "recaps/f1",     "yt_official": {"channelId": "UCB_qr75-ydFVKSF9Dmo6izg"}},  # :contentReference[oaicite:18]{index=18}
    "nascar": {"sport_path": "racing/nascar",   "out_dir": "recaps/nascar", "yt_official": {"channelQuery": "NASCAR"}},
    "ufc":    {"sport_path": "mma/ufc",         "out_dir": "recaps/ufc",    "yt_official": {"channelId": "UCvgfXK4nTYKudb0rFR6noLA"}},  # :contentReference[oaicite:19]{index=19}

    # Golf (ESPN has golf scoreboards; highlights rights vary, so default None)
    "pga":  {"sport_path": "golf/pga",  "out_dir": "recaps/pga",  "yt_official": None},
    "lpga": {"sport_path": "golf/lpga", "out_dir": "recaps/lpga", "yt_official": None},
}

# -----------------------------
# YouTube highlight matching
# -----------------------------
PREFERRED_CHANNEL_KEYWORDS = [
    "NBA", "NHL", "NFL", "MLB", "UEFA", "Premier League", "LALIGA", "Bundesliga", "Serie A", "Ligue 1",
    "Major League Soccer", "MLS", "UFC", "Formula 1", "NASCAR",
]

HIGHLIGHT_NEGATIVE_KEYWORDS = [
    "podcast", "reaction", "reacts", "full game", "full match", "press conference",
    "postgame", "interview", "highlights live",
]

CACHE_DIR = ".cache"
YT_CHANNEL_CACHE_PATH = os.path.join(CACHE_DIR, "youtube_channels.json")

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def write_json(path: str, data: Dict[str, Any]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()

def read_json(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def get_json(url: str, session: requests.Session) -> Dict[str, Any]:
    r = session.get(url, timeout=40)
    r.raise_for_status()
    return r.json()

def get_html(url: str, session: requests.Session) -> str:
    r = session.get(url, timeout=40)
    r.raise_for_status()
    return r.text

def clean_text(t: str) -> str:
    t = re.sub(r"\s+", " ", t).strip()
    return t

def extract_main_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    article = soup.find("article")
    if article:
        return clean_text(article.get_text(" "))

    body = soup.find("body")
    if body:
        return clean_text(body.get_text(" "))

    return clean_text(soup.get_text(" "))

def scoreboard_url(sport_path: str, yyyymmdd: str) -> str:
    return f"{ESPN_SITE_API}/sports/{sport_path}/scoreboard?dates={yyyymmdd}"

def summary_url(sport_path: str, event_id: str) -> str:
    return f"{ESPN_SITE_API}/sports/{sport_path}/summary?event={event_id}"

def extract_scoreline_from_summary(summary: Dict[str, Any]) -> Optional[str]:
    header = summary.get("header") or {}
    comps = header.get("competitions") or []
    if not comps:
        return None

    competitors = comps[0].get("competitors") or []
    if len(competitors) < 2:
        return None

    home = next((c for c in competitors if c.get("homeAway") == "home"), None)
    away = next((c for c in competitors if c.get("homeAway") == "away"), None)
    if not home or not away:
        return None

    home_team = (home.get("team") or {})
    away_team = (away.get("team") or {})

    home_abbr = home_team.get("abbreviation") or home_team.get("shortDisplayName") or "HOME"
    away_abbr = away_team.get("abbreviation") or away_team.get("shortDisplayName") or "AWAY"

    home_score = home.get("score")
    away_score = away.get("score")
    if home_score is None or away_score is None:
        return None

    return f"{away_abbr} {away_score} — {home_abbr} {home_score}"

def extract_team_display_names(summary: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    header = summary.get("header") or {}
    comps = header.get("competitions") or []
    if not comps:
        return (None, None)

    competitors = comps[0].get("competitors") or []
    if len(competitors) < 2:
        return (None, None)

    home = next((c for c in competitors if c.get("homeAway") == "home"), None)
    away = next((c for c in competitors if c.get("homeAway") == "away"), None)
    if not home or not away:
        return (None, None)

    home_team = (home.get("team") or {})
    away_team = (away.get("team") or {})

    home_name = home_team.get("displayName") or home_team.get("shortDisplayName")
    away_name = away_team.get("displayName") or away_team.get("shortDisplayName")
    return (away_name, home_name)

def extract_gamecast_url(summary: Dict[str, Any]) -> Optional[str]:
    header = summary.get("header") or {}
    links = header.get("links") or []
    for lk in links:
        rel = lk.get("rel") or []
        if isinstance(rel, list) and "gamecast" in rel:
            return lk.get("href")
        if (lk.get("text") or "").lower() == "gamecast":
            return lk.get("href")
    return None

def extract_recap_article_url(summary: Dict[str, Any]) -> Optional[str]:
    articles = summary.get("articles") or []
    if not articles:
        return None
    first = articles[0] or {}
    href = (((first.get("links") or {}).get("web") or {}).get("href"))
    return href

# -----------------------------
# OpenAI summarization
# -----------------------------
def openai_chat_completion(user_text: str) -> str:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")

    model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": user_text}],
        "temperature": 0.7,
    }
    r = requests.post(url, headers=headers, json=payload, timeout=90)
    r.raise_for_status()
    data = r.json()
    return data["choices"][0]["message"]["content"].strip()

# -----------------------------
# YouTube helpers
# -----------------------------
def _norm_tokens(s: str) -> List[str]:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    parts = [p for p in s.split() if p]
    stop = {"the", "vs", "v", "and", "at", "highlights", "highlight", "game", "full", "recap"}
    return [p for p in parts if p not in stop]

def _contains_both_teams(title: str, desc: str, away: str, home: str) -> bool:
    hay = f"{title} {desc}".lower()
    away_tokens = _norm_tokens(away)
    home_tokens = _norm_tokens(home)
    away_ok = any(t in hay for t in away_tokens[:3])
    home_ok = any(t in hay for t in home_tokens[:3])
    return away_ok and home_ok

def _looks_like_highlights(title: str, desc: str) -> bool:
    hay = f"{title} {desc}".lower()
    if "highlights" in hay or "game recap" in hay or "recap" in hay:
        for neg in HIGHLIGHT_NEGATIVE_KEYWORDS:
            if neg in hay:
                return False
        return True
    return False

def yt_cache_load() -> Dict[str, str]:
    ensure_dir(CACHE_DIR)
    return read_json(YT_CHANNEL_CACHE_PATH) or {}

def yt_cache_save(cache: Dict[str, str]) -> None:
    ensure_dir(CACHE_DIR)
    write_json(YT_CHANNEL_CACHE_PATH, cache)

def youtube_resolve_channel_id(channel_query: str) -> Optional[str]:
    """
    Resolve a channelId from a human query (ex: "Premier League", "NASCAR", "WNBA").
    Caches result in .cache/youtube_channels.json
    """
    yt_key = os.environ.get("YOUTUBE_API_KEY")
    if not yt_key:
        return None

    cache = yt_cache_load()
    if channel_query in cache:
        return cache[channel_query]

    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "key": yt_key,
        "part": "snippet",
        "q": channel_query,
        "type": "channel",
        "maxResults": 1,
        "safeSearch": "none",
    }
    r = requests.get(url, params=params, timeout=40)
    r.raise_for_status()
    data = r.json()
    items = data.get("items") or []
    if not items:
        return None

    ch_id = ((items[0].get("id") or {}).get("channelId"))
    if not ch_id:
        return None

    cache[channel_query] = ch_id
    yt_cache_save(cache)
    return ch_id

def youtube_search_highlight(
    away_team: str,
    home_team: str,
    date_iso: str,
    official_channel: Optional[Dict[str, str]],
) -> Optional[Dict[str, Any]]:
    """
    Searches ONLY within an official league channel (channelId).
    If official_channel has channelQuery, we resolve->cache channelId.
    """
    yt_key = os.environ.get("YOUTUBE_API_KEY")
    if not yt_key:
        return None

    channel_id = None
    if official_channel:
        channel_id = official_channel.get("channelId")
        if not channel_id and official_channel.get("channelQuery"):
            channel_id = youtube_resolve_channel_id(official_channel["channelQuery"])

    if not channel_id:
        return None  # strict: no official channel => no highlight

    nice_date = date_iso
    q = f"{away_team} vs {home_team} highlights {nice_date}"

    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "key": yt_key,
        "part": "snippet",
        "q": q,
        "type": "video",
        "maxResults": 8,
        "safeSearch": "none",
        "videoEmbeddable": "true",
        "order": "relevance",
        "channelId": channel_id,  # THIS is the “official account only” filter
    }

    r = requests.get(url, params=params, timeout=40)
    r.raise_for_status()
    data = r.json()

    items = data.get("items") or []
    if not items:
        return None

    best = None
    best_score = -1

    for it in items:
        vid = ((it.get("id") or {}).get("videoId"))
        sn = it.get("snippet") or {}
        title = sn.get("title") or ""
        desc = sn.get("description") or ""
        channel = sn.get("channelTitle") or ""
        published = sn.get("publishedAt")
        thumbs = sn.get("thumbnails") or {}
        thumb = None
        for k in ["high", "medium", "default"]:
            if k in thumbs and "url" in thumbs[k]:
                thumb = thumbs[k]["url"]
                break

        score = 0
        if _looks_like_highlights(title, desc):
            score += 6
        if _contains_both_teams(title, desc, away_team, home_team):
            score += 8
        if any(pk.lower() in channel.lower() for pk in PREFERRED_CHANNEL_KEYWORDS):
            score += 1
        if " vs " in title.lower() or " v " in title.lower():
            score += 1

        if score > best_score and vid:
            best_score = score
            best = {
                "query": q,
                "channelId": channel_id,
                "videoId": vid,
                "title": title,
                "channelTitle": channel,
                "publishedAt": published,
                "thumbnail": thumb,
                "watchUrl": f"https://www.youtube.com/watch?v={vid}",
                "embedUrl": f"https://www.youtube.com/embed/{vid}",
                "score": score,
            }

    # keep strict-ish
    if best and best["score"] >= 10:
        return best
    return None

# -----------------------------
# Build day
# -----------------------------
def build_league_day(league_key: str, yyyymmdd: str, mode: str, prompt_rules: str) -> Dict[str, Any]:
    league_cfg = LEAGUES[league_key]
    sport_path = league_cfg["sport_path"]

    session = requests.Session()
    session.headers.update({"User-Agent": DEFAULT_UA})

    sb = get_json(scoreboard_url(sport_path, yyyymmdd), session)
    events = sb.get("events", []) or []

    games_out: List[Dict[str, Any]] = []
    finished_recaps: List[str] = []

    date_iso = f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"

    for ev in events:
        event_id = ev.get("id")
        if not event_id:
            continue

        summ = get_json(summary_url(sport_path, event_id), session)

        scoreline = extract_scoreline_from_summary(summ)
        recap_url = extract_recap_article_url(summ)
        gamecast_url = extract_gamecast_url(summ)

        away_name, home_name = extract_team_display_names(summ)

        game_obj: Dict[str, Any] = {
            "eventId": event_id,
            "scoreLine": scoreline,
            "teams": {"away": away_name, "home": home_name},
            "source": {"recapUrl": recap_url, "gamecastUrl": gamecast_url},
        }

        # Strict official-channel highlights only
        official = league_cfg.get("yt_official")
        if official and away_name and home_name:
            try:
                yt = youtube_search_highlight(away_name, home_name, date_iso, official)
            except Exception:
                yt = None
            game_obj["highlight"] = yt
        else:
            game_obj["highlight"] = None

        if not recap_url:
            game_obj["status"] = "no_recap_url"
            games_out.append(game_obj)
            continue

        html = get_html(recap_url, session)
        article_text = extract_main_text_from_html(html)
        game_obj["articleText"] = article_text  # you can remove later

        if mode == "inputs":
            game_obj["status"] = "needs_manual_summary"
            game_obj["recapInput"] = f"{prompt_rules}\n\nHere is the article:\n\n{article_text}\n"
        else:
            game_obj["status"] = "ok"
            recap = openai_chat_completion(f"{prompt_rules}\n\nHere is the article:\n\n{article_text}\n")
            game_obj["recap"] = recap
            finished_recaps.append(recap)

        games_out.append(game_obj)
        time.sleep(0.35)

    league_briefing = None
    if mode == "openai" and finished_recaps:
        league_briefing = openai_chat_completion(
            "Create a THPORTH-style 'league in 30 seconds' briefing for this league/day.\n"
            "Rules:\n"
            "- Start with a rewritten headline in sentence case.\n"
            "- ONE short lead sentence.\n"
            "- 4–6 bullets with the biggest storylines (each bullet includes a key stat).\n"
            "- No play-by-play.\n\n"
            "Use these game recaps as your source:\n\n"
            + "\n\n---\n\n".join(finished_recaps)
        )

    return {
        "league": league_key,
        "date": yyyymmdd,
        "generatedAt": datetime.now().isoformat(),
        "mode": mode,
        "leagueBriefing": league_briefing,
        "games": games_out,
    }

def write_latest_and_index(out_dir: str, day_json: Dict[str, Any], keep_days: int = 14) -> None:
    write_json(os.path.join(out_dir, "latest.json"), day_json)

    dates = []
    for fn in os.listdir(out_dir):
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}\.json", fn):
            dates.append(fn.replace(".json", ""))
    dates.sort(reverse=True)
    dates = dates[:keep_days]
    write_json(os.path.join(out_dir, "index.json"), {"dates": dates})

def write_manifest() -> None:
    """
    A single manifest so the front-end can render ALL leagues without hardcoding.
    """
    manifest = {
        "generatedAt": datetime.now().isoformat(),
        "leagues": [
            {
                "key": k,
                "outDir": v["out_dir"],
                "sportPath": v["sport_path"],
                "hasOfficialHighlights": bool(v.get("yt_official")),
            }
            for k, v in LEAGUES.items()
        ],
    }
    write_json("recaps/manifest.json", manifest)

def write_briefings_latest(league_days: List[Dict[str, Any]]) -> None:
    """
    briefings/latest.json aggregates per-league briefings for the day.
    """
    out = {
        "generatedAt": datetime.now().isoformat(),
        "leagues": [
            {"league": d["league"], "date": d["date"], "briefing": d.get("leagueBriefing")}
            for d in league_days
        ],
    }
    write_json("briefings/latest.json", out)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True, choices=sorted(LEAGUES.keys()) + ["all"])
    ap.add_argument("--date", required=True, help="YYYYMMDD")
    ap.add_argument("--mode", choices=["inputs", "openai"], default="inputs")
    ap.add_argument("--prompt", default="scripts/thporth_prompt.txt")
    args = ap.parse_args()

    prompt_rules = read_text(args.prompt)

    league_days: List[Dict[str, Any]] = []

    if args.league == "all":
        for league_key in sorted(LEAGUES.keys()):
            day = build_league_day(league_key, args.date, args.mode, prompt_rules)
            out_dir = LEAGUES[league_key]["out_dir"]
            ensure_dir(out_dir)

            date_iso = f"{args.date[:4]}-{args.date[4:6]}-{args.date[6:]}"
            out_path = os.path.join(out_dir, f"{date_iso}.json")
            write_json(out_path, day)
            write_latest_and_index(out_dir, day, keep_days=14)

            league_days.append(day)
            print(f"Wrote {out_path} and updated latest.json/index.json")

        write_manifest()
        write_briefings_latest(league_days)
        print("Wrote recaps/manifest.json and briefings/latest.json")
        return

    # single league
    day = build_league_day(args.league, args.date, args.mode, prompt_rules)
    out_dir = LEAGUES[args.league]["out_dir"]
    ensure_dir(out_dir)

    date_iso = f"{args.date[:4]}-{args.date[4:6]}-{args.date[6:]}"
    out_path = os.path.join(out_dir, f"{date_iso}.json")
    write_json(out_path, day)
    write_latest_and_index(out_dir, day, keep_days=14)

    write_manifest()
    write_briefings_latest([day])

    print(f"Wrote {out_path} and updated latest.json/index.json + manifest + briefings")

if __name__ == "__main__":
    main()
