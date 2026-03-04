#!/usr/bin/env python3
import os
import re
import json
import time
import argparse
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

ESPN_SITE_API = "https://site.api.espn.com/apis/site/v2"

DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

CACHE_DIR = ".cache"
YT_CHANNEL_CACHE_PATH = os.path.join(CACHE_DIR, "youtube_channels.json")

# =========================
# NBA ONLY (all other leagues commented out)
# =========================
LEAGUES: Dict[str, Dict[str, Any]] = {
    "nba": {
        "sport_path": "basketball/nba",
        "out_dir": "recaps/nba",
        # Official NBA YouTube channel
        "yt_official": {"channelId": "UCWJ2lWNubArHWmf3FIHbfcQ"},
    },

    # ---- COMMENTED OUT FOR NOW ----
    # "nhl":  {"sport_path": "hockey/nhl",     "out_dir": "recaps/nhl",  "yt_official": {"channelId": "UCqFMzb-4AUf6WAIbl132QKA"}},
    # "mlb":  {"sport_path": "baseball/mlb",   "out_dir": "recaps/mlb",  "yt_official": {"channelId": "UCoLrcjPV5PbUrUyXq5mjc_A"}},
    # "nfl":  {"sport_path": "football/nfl",   "out_dir": "recaps/nfl",  "yt_official": {"channelId": "UCDVYQ4Zhbm3S2dlz7P1GBDg"}},
    # "wnba": {"sport_path": "basketball/wnba","out_dir": "recaps/wnba", "yt_official": {"channelQuery": "WNBA"}},
    # "ncaam":  {"sport_path": "basketball/mens-college-basketball",   "out_dir": "recaps/ncaam",  "yt_official": None},
    # "ncaawb": {"sport_path": "basketball/womens-college-basketball", "out_dir": "recaps/ncaawb", "yt_official": None},
    # "ncaaf":  {"sport_path": "football/college-football",           "out_dir": "recaps/ncaaf",  "yt_official": None},
    # "epl":    {"sport_path": "soccer/eng.1",            "out_dir": "recaps/epl",    "yt_official": {"channelQuery": "Premier League"}},
    # "mls":    {"sport_path": "soccer/usa.1",            "out_dir": "recaps/mls",    "yt_official": {"channelId": "UCSZbXT5TLLW_i-5W8FZpFsg"}},
    # "laliga": {"sport_path": "soccer/esp.1",            "out_dir": "recaps/laliga", "yt_official": {"channelQuery": "LALIGA"}},
    # "seriea": {"sport_path": "soccer/ita.1",            "out_dir": "recaps/seriea", "yt_official": {"channelQuery": "Serie A"}},
    # "bund":   {"sport_path": "soccer/ger.1",            "out_dir": "recaps/bund",   "yt_official": {"channelQuery": "Bundesliga"}},
    # "ligue1": {"sport_path": "soccer/fra.1",            "out_dir": "recaps/ligue1", "yt_official": {"channelQuery": "Ligue 1"}},
    # "ucl":    {"sport_path": "soccer/uefa.champions",   "out_dir": "recaps/ucl",    "yt_official": {"channelId": "UCyGa1YEx9ST66rYrJTGIKOw"}},
    # "uel":    {"sport_path": "soccer/uefa.europa",      "out_dir": "recaps/uel",    "yt_official": {"channelId": "UCyGa1YEx9ST66rYrJTGIKOw"}},
    # "uecl":   {"sport_path": "soccer/uefa.europa.conf", "out_dir": "recaps/uecl",   "yt_official": {"channelId": "UCyGa1YEx9ST66rYrJTGIKOw"}},
    # "f1":     {"sport_path": "racing/f1",     "out_dir": "recaps/f1",     "yt_official": {"channelId": "UCB_qr75-ydFVKSF9Dmo6izg"}},
    # "nascar": {"sport_path": "racing/nascar", "out_dir": "recaps/nascar", "yt_official": {"channelQuery": "NASCAR"}},
    # "ufc":    {"sport_path": "mma/ufc",       "out_dir": "recaps/ufc",    "yt_official": {"channelId": "UCvgfXK4nTYKudb0rFR6noLA"}},
    # "pga":  {"sport_path": "golf/pga",  "out_dir": "recaps/pga",  "yt_official": None},
    # "lpga": {"sport_path": "golf/lpga", "out_dir": "recaps/lpga", "yt_official": None},
}

HIGHLIGHT_NEGATIVE_KEYWORDS = [
    "podcast", "reaction", "reacts", "full game", "full match", "press conference",
    "postgame", "interview", "highlights live",
]


def ensure_dir(p: str) -> None:
    if p:
        os.makedirs(p, exist_ok=True)


def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def write_json(path: str, data: Dict[str, Any]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def read_json(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def clean_text(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "")).strip()


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


def get_json_url(url: str, session: requests.Session) -> Optional[Dict[str, Any]]:
    r = session.get(url, timeout=40)
    if r.status_code in (400, 404):
        return None
    r.raise_for_status()
    return r.json()


def get_html_url(url: str, session: requests.Session) -> str:
    r = session.get(url, timeout=40)
    r.raise_for_status()
    return r.text


def scoreboard_url(sport_path: str, yyyymmdd: str) -> str:
    return f"{ESPN_SITE_API}/sports/{sport_path}/scoreboard?dates={yyyymmdd}"


def summary_url(sport_path: str, event_id: str) -> str:
    return f"{ESPN_SITE_API}/sports/{sport_path}/summary?event={event_id}"


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


def extract_scoreline(summary: Dict[str, Any]) -> Optional[str]:
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
    return (((first.get("links") or {}).get("web") or {}).get("href"))


# -----------------------------
# OpenAI API calls (simple; you can add backoff later)
# -----------------------------
def openai_chat_completion(user_text: str, model: str, temperature: float = 0.7) -> str:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")

    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": user_text}],
        "temperature": temperature,
    }
    r = requests.post(url, headers=headers, json=payload, timeout=180)
    r.raise_for_status()
    data = r.json()
    return data["choices"][0]["message"]["content"].strip()


def safe_json_loads(text: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(text)
    except Exception:
        return None


# -----------------------------
# YouTube API helpers (official channel only)
# -----------------------------
def yt_cache_load() -> Dict[str, str]:
    ensure_dir(CACHE_DIR)
    return read_json(YT_CHANNEL_CACHE_PATH) or {}


def yt_cache_save(cache: Dict[str, str]) -> None:
    ensure_dir(CACHE_DIR)
    write_json(YT_CHANNEL_CACHE_PATH, cache)


def youtube_resolve_channel_id(channel_query: str) -> Optional[str]:
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


def youtube_search_highlight_official(
    away_team: str,
    home_team: str,
    date_iso: str,
    official: Optional[Dict[str, str]],
) -> Optional[Dict[str, Any]]:
    yt_key = os.environ.get("YOUTUBE_API_KEY")
    if not yt_key:
        return None

    if not official:
        return None

    channel_id = official.get("channelId")
    if not channel_id and official.get("channelQuery"):
        channel_id = youtube_resolve_channel_id(official["channelQuery"])

    if not channel_id:
        return None

    q = f"{away_team} vs {home_team} highlights {date_iso}"

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
        "channelId": channel_id,
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

    if best and best["score"] >= 10:
        return best
    return None


def build_league_day(
    league_key: str,
    yyyymmdd: str,
    mode: str,
    thporth_prompt: str,
    extractor_prompt: str,
    briefing_prompt: str,
    extractor_model: str,
    writer_model: str,
) -> Dict[str, Any]:
    cfg = LEAGUES[league_key]
    sport_path = cfg["sport_path"]
    out_dir = cfg["out_dir"]
    official = cfg.get("yt_official")

    session = requests.Session()
    session.headers.update({"User-Agent": DEFAULT_UA})

    sb_url = scoreboard_url(sport_path, yyyymmdd)
    sb = get_json_url(sb_url, session)

    date_iso = f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"

    if not sb:
        day_json: Dict[str, Any] = {
            "league": league_key,
            "date": yyyymmdd,
            "generatedAt": datetime.now().isoformat(),
            "mode": mode,
            "leagueBriefing": None,
            "games": [],
            "status": "skipped_no_scoreboard",
            "scoreboardUrl": sb_url,
        }
        ensure_dir(out_dir)
        write_json(os.path.join(out_dir, f"{date_iso}.json"), day_json)
        write_json(os.path.join(out_dir, "latest.json"), day_json)
        write_json(os.path.join(out_dir, "index.json"), {"dates": [date_iso]})
        return day_json

    events = sb.get("events", []) or []
    games_out: List[Dict[str, Any]] = []
    facts_for_briefing: List[Dict[str, Any]] = []

    for ev in events:
        event_id = ev.get("id")
        if not event_id:
            continue

        summ = get_json_url(summary_url(sport_path, event_id), session)
        if not summ:
            games_out.append({"eventId": event_id, "status": "summary_missing"})
            continue

        away_name, home_name = extract_team_display_names(summ)
        scoreline = extract_scoreline(summ)
        recap_url = extract_recap_article_url(summ)
        gamecast_url = extract_gamecast_url(summ)

        game_obj: Dict[str, Any] = {
            "eventId": event_id,
            "teams": {"away": away_name, "home": home_name},
            "scoreLine": scoreline,
            "source": {"recapUrl": recap_url, "gamecastUrl": gamecast_url},
            "status": "init",
        }

        # Highlights
        try:
            if away_name and home_name:
                game_obj["highlight"] = youtube_search_highlight_official(away_name, home_name, date_iso, official)
            else:
                game_obj["highlight"] = None
        except Exception:
            game_obj["highlight"] = None

        # FIX #1 fallback: use recapUrl if present else gamecastUrl
        fetch_url = recap_url or gamecast_url
        if not fetch_url:
            game_obj["status"] = "no_recap_url"
            games_out.append(game_obj)
            continue
        game_obj["source"]["usedTextFrom"] = fetch_url

        # Fetch text
        try:
            html = get_html_url(fetch_url, session)
            article_text = extract_main_text_from_html(html)
        except Exception:
            game_obj["status"] = "recap_fetch_failed"
            games_out.append(game_obj)
            continue

        game_obj["articleText"] = article_text

        if mode == "inputs":
            game_obj["status"] = "needs_manual_summary"
            game_obj["recapInput"] = thporth_prompt + "\n\nHere is the article:\n\n" + article_text + "\n"
            games_out.append(game_obj)
            continue

        extractor_input = (
            extractor_prompt +
            "\n\nKnown metadata:\n" +
            json.dumps({
                "league": league_key,
                "event_title": f"{away_name} @ {home_name}" if (away_name and home_name) else None,
                "scoreline": scoreline,
                "recapUrl": recap_url,
                "usedTextFrom": fetch_url,
            }, ensure_ascii=False) +
            "\n\nArticle text:\n" +
            article_text
        )

        facts_text = openai_chat_completion(extractor_input, model=extractor_model, temperature=0.2)
        facts_json = safe_json_loads(facts_text)
        if not facts_json:
            game_obj["status"] = "extractor_json_parse_failed"
            game_obj["factsRaw"] = facts_text
            games_out.append(game_obj)
            continue

        game_obj["facts"] = facts_json

        writer_input = (
            thporth_prompt +
            "\n\nIMPORTANT: Use ONLY the extracted facts JSON below. Do not invent details.\n\n" +
            "Extracted facts (JSON):\n" +
            json.dumps(facts_json, ensure_ascii=False) +
            "\n"
        )

        recap_text = openai_chat_completion(writer_input, model=writer_model, temperature=0.7)
        game_obj["recap"] = recap_text
        game_obj["status"] = "ok"

        games_out.append(game_obj)
        facts_for_briefing.append(facts_json)

        time.sleep(1.0)

    league_briefing = None
    if mode == "openai" and facts_for_briefing:
        briefing_input = (
            briefing_prompt +
            "\n\nLeague: " + league_key +
            "\nDate: " + date_iso +
            "\n\nGames facts JSON list:\n" +
            json.dumps(facts_for_briefing, ensure_ascii=False)
        )
        league_briefing = openai_chat_completion(briefing_input, model=writer_model, temperature=0.6)

    day_json: Dict[str, Any] = {
        "league": league_key,
        "date": yyyymmdd,
        "generatedAt": datetime.now().isoformat(),
        "mode": mode,
        "leagueBriefing": league_briefing,
        "games": games_out,
        "status": "ok",
    }

    ensure_dir(out_dir)
    write_json(os.path.join(out_dir, f"{date_iso}.json"), day_json)
    write_json(os.path.join(out_dir, "latest.json"), day_json)
    write_json(os.path.join(out_dir, "index.json"), {"dates": [date_iso]})

    return day_json


def write_manifest() -> None:
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
    ensure_dir("recaps")
    write_json("recaps/manifest.json", manifest)


def write_briefings_latest(all_days: List[Dict[str, Any]]) -> None:
    ensure_dir("briefings")
    out = {
        "generatedAt": datetime.now().isoformat(),
        "leagues": [
            {"league": d["league"], "date": d["date"], "briefing": d.get("leagueBriefing")}
            for d in all_days
        ],
    }
    write_json("briefings/latest.json", out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True, choices=sorted(LEAGUES.keys()) + ["all"])
    ap.add_argument("--date", required=True, help="YYYYMMDD")
    ap.add_argument("--mode", choices=["inputs", "openai"], default="openai")

    ap.add_argument("--thporth_prompt", default="scripts/thporth_prompt.txt")
    ap.add_argument("--extractor_prompt", default="scripts/extractor_prompt.txt")
    ap.add_argument("--briefing_prompt", default="scripts/briefing_prompt.txt")

    ap.add_argument("--extractor_model", default=os.environ.get("OPENAI_EXTRACTOR_MODEL", "gpt-5-mini"))
    ap.add_argument("--writer_model", default=os.environ.get("OPENAI_WRITER_MODEL", "gpt-5-mini"))

    args = ap.parse_args()

    thporth_prompt = read_text(args.thporth_prompt)
    extractor_prompt = read_text(args.extractor_prompt)
    briefing_prompt = read_text(args.briefing_prompt)

    if args.league == "all":
        # With NBA-only config, "all" == nba
        all_days: List[Dict[str, Any]] = []
        for league_key in sorted(LEAGUES.keys()):
            day = build_league_day(
                league_key=league_key,
                yyyymmdd=args.date,
                mode=args.mode,
                thporth_prompt=thporth_prompt,
                extractor_prompt=extractor_prompt,
                briefing_prompt=briefing_prompt,
                extractor_model=args.extractor_model,
                writer_model=args.writer_model,
            )
            all_days.append(day)
        write_manifest()
        write_briefings_latest(all_days)
        return

    day = build_league_day(
        league_key=args.league,
        yyyymmdd=args.date,
        mode=args.mode,
        thporth_prompt=thporth_prompt,
        extractor_prompt=extractor_prompt,
        briefing_prompt=briefing_prompt,
        extractor_model=args.extractor_model,
        writer_model=args.writer_model,
    )
    write_manifest()
    write_briefings_latest([day])


if __name__ == "__main__":
    main()
