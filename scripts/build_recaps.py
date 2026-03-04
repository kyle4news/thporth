#!/usr/bin/env python3
import os
import re
import json
import time
import argparse
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

import requests
from bs4 import BeautifulSoup

ESPN_SITE_API = "https://site.api.espn.com/apis/site/v2"

DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

LEAGUES = {
    # Add more later; this is enough to prove it works.
    "nba": {"sport_path": "basketball/nba", "out_dir": "recaps/nba"},
    "nhl": {"sport_path": "hockey/nhl", "out_dir": "recaps/nhl"},
}

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def write_json(path: str, data: Dict[str, Any]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()

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

def extract_gamecast_url(summary: Dict[str, Any]) -> Optional[str]:
    # ESPN summary sometimes includes a "links" object with a gamecast.
    header = summary.get("header") or {}
    links = header.get("links") or []
    for lk in links:
        if (lk.get("rel") and "gamecast" in lk.get("rel")) or lk.get("text", "").lower() == "gamecast":
            return lk.get("href")
    return None

def extract_recap_article_url(summary: Dict[str, Any]) -> Optional[str]:
    articles = summary.get("articles") or []
    if not articles:
        return None
    first = articles[0] or {}
    href = (((first.get("links") or {}).get("web") or {}).get("href"))
    return href

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

def build_league_day(league_key: str, yyyymmdd: str, mode: str, prompt_rules: str) -> Dict[str, Any]:
    league_cfg = LEAGUES[league_key]
    sport_path = league_cfg["sport_path"]

    session = requests.Session()
    session.headers.update({"User-Agent": DEFAULT_UA})

    sb = get_json(scoreboard_url(sport_path, yyyymmdd), session)
    events = sb.get("events", []) or []

    games_out: List[Dict[str, Any]] = []
    finished_recaps: List[str] = []

    for ev in events:
        event_id = ev.get("id")
        if not event_id:
            continue

        summ = get_json(summary_url(sport_path, event_id), session)

        scoreline = extract_scoreline_from_summary(summ)
        recap_url = extract_recap_article_url(summ)
        gamecast_url = extract_gamecast_url(summ)

        game_obj: Dict[str, Any] = {
            "eventId": event_id,
            "scoreLine": scoreline,
            "source": {"recapUrl": recap_url, "gamecastUrl": gamecast_url},
        }

        if not recap_url:
            game_obj["status"] = "no_recap_url"
            games_out.append(game_obj)
            continue

        html = get_html(recap_url, session)
        article_text = extract_main_text_from_html(html)
        game_obj["articleText"] = article_text  # keep for debugging; remove later if you want smaller files

        if mode == "inputs":
            game_obj["status"] = "needs_manual_summary"
            game_obj["recapInput"] = (
                f"{prompt_rules}\n\n"
                f"Here is the article:\n\n{article_text}\n"
            )
        else:
            game_obj["status"] = "ok"
            recap = openai_chat_completion(
                f"{prompt_rules}\n\nHere is the article:\n\n{article_text}\n"
            )
            game_obj["recap"] = recap
            finished_recaps.append(recap)

        games_out.append(game_obj)

        # be kind to ESPN
        time.sleep(0.4)

    league_briefing = None
    if mode == "openai" and finished_recaps:
        league_briefing = openai_chat_completion(
            "Create a THPORTH-style 'league in 30 seconds' morning briefing for this league/day.\n"
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

def write_latest_and_index(out_dir: str, date_iso: str, day_json: Dict[str, Any], keep_days: int = 14) -> None:
    # latest.json
    write_json(os.path.join(out_dir, "latest.json"), day_json)

    # index.json (simple list of dates we have on disk)
    # scan out_dir for YYYY-MM-DD.json
    dates = []
    for fn in os.listdir(out_dir):
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}\.json", fn):
            dates.append(fn.replace(".json", ""))
    dates.sort(reverse=True)
    dates = dates[:keep_days]

    write_json(os.path.join(out_dir, "index.json"), {"dates": dates})

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True, choices=sorted(LEAGUES.keys()))
    ap.add_argument("--date", required=True, help="YYYYMMDD")
    ap.add_argument("--mode", choices=["inputs", "openai"], default="inputs")
    ap.add_argument("--prompt", default="scripts/thporth_prompt.txt")
    args = ap.parse_args()

    prompt_rules = read_text(args.prompt)

    day = build_league_day(args.league, args.date, args.mode, prompt_rules)

    out_dir = LEAGUES[args.league]["out_dir"]
    ensure_dir(out_dir)

    date_iso = f"{args.date[:4]}-{args.date[4:6]}-{args.date[6:]}"
    out_path = os.path.join(out_dir, f"{date_iso}.json")
    write_json(out_path, day)
    write_latest_and_index(out_dir, date_iso, day, keep_days=14)

    print(f"Wrote {out_path} and updated latest.json/index.json")

if __name__ == "__main__":
    main()
