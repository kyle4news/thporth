#!/usr/bin/env python3
import os
import re
import json
import time
import argparse
import random
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import requests

ESPN_SITE_API = "https://site.api.espn.com/apis/site/v2"

DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

CACHE_DIR = ".cache"
YT_CHANNEL_CACHE_PATH = os.path.join(CACHE_DIR, "youtube_channels.json")

# =========================
# NBA ONLY
# =========================
LEAGUES: Dict[str, Dict[str, Any]] = {
    "nba": {
        "sport_path": "basketball/nba",
        "out_dir": "recaps/nba",
        # Official NBA YouTube channel
        "yt_official": {"channelId": "UCWJ2lWNubArHWmf3FIHbfcQ"},
    }
}

HIGHLIGHT_NEGATIVE_KEYWORDS = [
    "podcast", "reaction", "reacts", "full game", "full match", "press conference",
    "postgame", "interview", "highlights live", "final", "final minutes"
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


def get_json_url(url: str, session: requests.Session) -> Optional[Dict[str, Any]]:
    r = session.get(url, timeout=40)
    if r.status_code in (400, 404):
        return None
    r.raise_for_status()
    return r.json()


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


# =========================
# FACTS BUILDER (NO ARTICLES)
# =========================
def build_nba_facts_from_summary(summary: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a compact "facts JSON" from ESPN summary JSON.
    This is what we feed to OpenAI so we only need 1 call per game.
    """
    facts: Dict[str, Any] = {}

    header = summary.get("header") or {}
    comps = header.get("competitions") or []
    comp = comps[0] if comps else {}

    competitors = comp.get("competitors") or []
    home = next((c for c in competitors if c.get("homeAway") == "home"), None)
    away = next((c for c in competitors if c.get("homeAway") == "away"), None)

    def team_blob(c):
        if not c:
            return None
        t = (c.get("team") or {})
        return {
            "displayName": t.get("displayName"),
            "shortDisplayName": t.get("shortDisplayName"),
            "abbreviation": t.get("abbreviation"),
            "score": c.get("score"),
            "winner": c.get("winner"),
            "homeAway": c.get("homeAway"),
            "records": c.get("records"),
        }

    facts["eventId"] = summary.get("id") or header.get("id")
    facts["shortHeadline"] = header.get("shortHeadline")
    facts["gameNote"] = header.get("gameNote")

    facts["teams"] = {
        "away": team_blob(away),
        "home": team_blob(home),
    }

    status = (comp.get("status") or {}).get("type") or {}
    facts["status"] = {
        "state": status.get("state"),
        "completed": status.get("completed"),
        "description": status.get("description"),
        "detail": status.get("detail"),
    }

    # Linescore
    def lines(c):
        if not c:
            return None
        ls = c.get("linescores") or []
        out = []
        for x in ls:
            if isinstance(x, dict) and "value" in x:
                out.append(x["value"])
        return out

    facts["linescore"] = {
        "away": lines(away),
        "home": lines(home),
    }

    # Team totals + player totals in boxscore (trimmed)
    box = summary.get("boxscore") or {}
    facts["boxscore"] = {
        "teams": box.get("teams"),
        "players": None,  # we’ll add a trimmed version below
    }

    # Leaders (often present)
    facts["leaders"] = summary.get("leaders")

    # Notes (sometimes include streak/milestones)
    facts["notes"] = summary.get("notes")

    # Trim players (boxscore.players can be huge)
    players = box.get("players") or []
    trimmed_players = []
    for team_entry in players:
        try:
            t = team_entry.get("team") or {}
            stats_cats = team_entry.get("statistics") or []
            # Keep only the first 2 categories to reduce size (usually "starters"/"bench" or similar)
            keep_cats = stats_cats[:2]
            trimmed_players.append({
                "team": {
                    "displayName": t.get("displayName"),
                    "abbreviation": t.get("abbreviation"),
                },
                "statistics": keep_cats,
            })
        except Exception:
            continue

    facts["boxscore"]["players"] = trimmed_players

    return facts


# =========================
# OPENAI WITH RETRY/BACKOFF
# =========================
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

    max_attempts = 10
    base_sleep = 2.0

    last_status = None
    last_body = None

    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=180)
            last_status = r.status_code
            last_body = r.text[:500] if r.text else None

            if r.status_code == 200:
                data = r.json()
                return data["choices"][0]["message"]["content"].strip()

            # Retry on rate limit / transient server errors
            if r.status_code in (429, 500, 502, 503, 504):
                retry_after = r.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except Exception:
                        sleep_s = base_sleep * (2 ** (attempt - 1))
                else:
                    sleep_s = base_sleep * (2 ** (attempt - 1))

                # jitter
                sleep_s = min(120.0, sleep_s) + random.random()
                print(f"OpenAI {r.status_code} - retrying in {sleep_s:.1f}s (attempt {attempt}/{max_attempts})")
                time.sleep(sleep_s)
                continue

            # Non-retriable
            r.raise_for_status()

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            sleep_s = min(120.0, base_sleep * (2 ** (attempt - 1))) + random.random()
            print(f"OpenAI network error: {e}. Retrying in {sleep_s:.1f}s (attempt {attempt}/{max_attempts})")
            time.sleep(sleep_s)
            continue

    raise RuntimeError(f"OpenAI failed after retries. last_status={last_status} last_body={last_body}")


# =========================
# YOUTUBE (OFFICIAL CHANNEL ONLY)
# =========================
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
    stop = {"the", "vs", "v", "and", "at", "highlights", "highlight", "game", "recap"}
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
    if "highlights" not in hay:
        return False
    for neg in HIGHLIGHT_NEGATIVE_KEYWORDS:
        if neg in hay:
            return False
    return True


def _published_within_days(published_at: Optional[str], date_iso: str, days: int = 30) -> bool:
    if not published_at:
        return False
    try:
        pub = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        game = datetime.fromisoformat(date_iso + "T00:00:00+00:00")
        return abs((pub - game).days) <= days
    except Exception:
        return False


def youtube_search_highlight_official(
    away_team: str,
    home_team: str,
    date_iso: str,
    official: Optional[Dict[str, str]],
) -> Optional[Dict[str, Any]]:
    yt_key = os.environ.get("YOUTUBE_API_KEY")
    if not yt_key or not official:
        return None

    channel_id = official.get("channelId")
    if not channel_id and official.get("channelQuery"):
        channel_id = youtube_resolve_channel_id(official["channelQuery"])
    if not channel_id:
        return None

    q = f"{away_team} vs {home_team} highlights"

    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "key": yt_key,
        "part": "snippet",
        "q": q,
        "type": "video",
        "maxResults": 10,
        "safeSearch": "none",
        "videoEmbeddable": "true",
        "order": "date",
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
        if _published_within_days(published, date_iso, days=30):
            score += 4

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

    # strict threshold: must look like highlights + match both teams, ideally date-close
    if best and best["score"] >= 14:
        return best
    return None


# =========================
# BUILD LOGIC
# =========================
def build_league_day(
    league_key: str,
    yyyymmdd: str,
    mode: str,
    thporth_prompt: str,
    briefing_prompt: str,
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
        gamecast_url = extract_gamecast_url(summ)

        # Build facts locally from summary JSON
        facts_json = build_nba_facts_from_summary(summ)

        game_obj: Dict[str, Any] = {
            "eventId": event_id,
            "teams": {"away": away_name, "home": home_name},
            "scoreLine": scoreline,
            "source": {"gamecastUrl": gamecast_url},
            "facts": facts_json,
            "status": "init",
        }

        # Highlights (official channel only)
        try:
            if away_name and home_name:
                game_obj["highlight"] = youtube_search_highlight_official(away_name, home_name, date_iso, official)
            else:
                game_obj["highlight"] = None
        except Exception:
            game_obj["highlight"] = None

        if mode == "inputs":
            game_obj["status"] = "needs_manual_summary"
            game_obj["recapInput"] = (
                thporth_prompt
                + "\n\nFacts JSON:\n"
                + json.dumps(facts_json, ensure_ascii=False)
            )
            games_out.append(game_obj)
            facts_for_briefing.append(facts_json)
            continue

        # ONE OpenAI call per game: generate recap from facts JSON
        writer_input = (
            thporth_prompt
            + "\n\nFacts JSON:\n"
            + json.dumps(facts_json, ensure_ascii=False)
        )

        try:
            recap_text = openai_chat_completion(writer_input, model=writer_model, temperature=0.7)
            game_obj["recap"] = recap_text
            game_obj["status"] = "ok"
            facts_for_briefing.append(facts_json)
        except Exception as e:
            game_obj["status"] = "openai_failed"
            game_obj["openaiError"] = str(e)

        games_out.append(game_obj)

        # Throttle (light) to reduce bursts; backoff handles real rate limiting
        time.sleep(1.25)

    # League briefing: ONE OpenAI call per league day
    league_briefing = None
    if mode == "openai" and facts_for_briefing:
        briefing_input = (
            briefing_prompt
            + "\n\nLeague: " + league_key
            + "\nDate: " + date_iso
            + "\n\nGames facts JSON list:\n"
            + json.dumps(facts_for_briefing, ensure_ascii=False)
        )
        try:
            league_briefing = openai_chat_completion(briefing_input, model=writer_model, temperature=0.6)
        except Exception:
            league_briefing = None

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
    ap.add_argument("--briefing_prompt", default="scripts/briefing_prompt.txt")

    ap.add_argument("--writer_model", default=os.environ.get("OPENAI_WRITER_MODEL", "gpt-5-mini"))

    args = ap.parse_args()

    thporth_prompt = read_text(args.thporth_prompt)
    briefing_prompt = read_text(args.briefing_prompt)

    all_days: List[Dict[str, Any]] = []

    if args.league == "all":
        for league_key in sorted(LEAGUES.keys()):
            day = build_league_day(
                league_key=league_key,
                yyyymmdd=args.date,
                mode=args.mode,
                thporth_prompt=thporth_prompt,
                briefing_prompt=briefing_prompt,
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
        briefing_prompt=briefing_prompt,
        writer_model=args.writer_model,
    )
    write_manifest()
    write_briefings_latest([day])


if __name__ == "__main__":
    main()
