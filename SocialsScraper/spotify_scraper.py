"""
spotify_scraper.py - Zero-Cost Local Spotify Analytics Scraper

Features:
- 100% Free: No Spotify Developer Account or Premium subscription required.
- TLS browser impersonation via curl_cffi to bypass Akamai/Cloudflare blocks.
- Anonymous Web Player token harvesting.
- Extracts: Monthly Listeners, Followers, Top 10 Play Counts, Top Cities, World Rank.
- Calculates: Stickiness Ratio %, Top Track Concentration %.
- Atomic SQLite persistence: metrics_cache (TTL) + historical_metrics (deduplicated deltas).
"""

import json
import logging
import re
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from curl_cffi import requests as cffi_requests
    HAS_CURL_CFFI = True
except ImportError:
    import requests as cffi_requests
    HAS_CURL_CFFI = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = "socials_cache.db"
TARGET_ARTIST_ID = "3fNvLxWjqCfeHwBYtHmuGI"  # K.LAW (Joshua Kalaw)

PATHFINDER_URL = "https://api-partner.spotify.com/pathfinder/v1/query"
ARTIST_OVERVIEW_HASHES = [
    "433e28d1e949372d3ca3aa6c47975cff428b5dc37b12f5325d9213accadf770a",
    "d66221ea13998b2f81883c5187d174c8646e4041d67f5b1e103bc262d447e3a0",
    "35a699e12a728c1a02f5bf67121a50f87341e65054e13126c03b7697fbd26692"
]

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://open.spotify.com",
    "Referer": "https://open.spotify.com/",
    "app-platform": "WebPlayer"
}


def _get_session():
    """Initializes session with Chrome TLS fingerprinting to bypass anti-bot blocks."""
    if HAS_CURL_CFFI:
        return cffi_requests.Session(impersonate="chrome124")
    session = cffi_requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


def fetch_anonymous_token(artist_id: str) -> Optional[str]:
    """Harvests an ephemeral anonymous Web Player token from the artist's embed page."""
    embed_url = f"https://open.spotify.com/embed/artist/{artist_id}"
    session = _get_session()

    try:
        response = session.get(embed_url, timeout=12)
        if response.status_code != 200:
            return None

        match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', response.text, re.DOTALL)
        if not match:
            return None

        data = json.loads(match.group(1))
        return (
            data.get("props", {})
            .get("pageProps", {})
            .get("state", {})
            .get("settings", {})
            .get("session", {})
            .get("accessToken")
        )
    except Exception as exc:
        logger.warning("Error retrieving anonymous token: %s", exc)
        return None


def fetch_artist_overview_graphql(artist_id: str, access_token: str) -> Optional[dict]:
    """Queries Spotify's internal Pathfinder GraphQL API using the anonymous Bearer token."""
    session = _get_session()
    headers = {
        **DEFAULT_HEADERS,
        "Authorization": f"Bearer {access_token}",
        "content-type": "application/json;charset=UTF-8"
    }

    variables = {
        "uri": f"spotify:artist:{artist_id}",
        "locale": "",
        "includePrerelease": True
    }

    for sha256_hash in ARTIST_OVERVIEW_HASHES:
        params = {
            "operationName": "queryArtistOverview",
            "variables": json.dumps(variables),
            "extensions": json.dumps({
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": sha256_hash
                }
            })
        }

        try:
            resp = session.get(PATHFINDER_URL, params=params, headers=headers, timeout=12)
            if resp.status_code == 200:
                result = resp.json()
                if "data" in result and result["data"]:
                    return result["data"]
            elif resp.status_code == 400:
                continue
        except Exception:
            continue

    return None


def fetch_artist_page_fallback(artist_id: str) -> Dict[str, int]:
    """Fallback: Regex parsing from public HTML if GraphQL hashes rotate."""
    artist_url = f"https://open.spotify.com/artist/{artist_id}"
    session = _get_session()
    metrics = {"monthly_listeners": 0}

    try:
        resp = session.get(artist_url, timeout=12)
        if resp.status_code == 200:
            match = re.search(r'([\d,\.]+[KkMmBb]?)\s+monthly\s+listeners', resp.text, re.IGNORECASE)
            if match:
                raw_str = match.group(1).replace(",", "").strip()
                if raw_str.upper().endswith("K"):
                    metrics["monthly_listeners"] = int(float(raw_str[:-1]) * 1_000)
                elif raw_str.upper().endswith("M"):
                    metrics["monthly_listeners"] = int(float(raw_str[:-1]) * 1_000_000)
                elif raw_str.upper().endswith("B"):
                    metrics["monthly_listeners"] = int(float(raw_str[:-1]) * 1_000_000_000)
                else:
                    metrics["monthly_listeners"] = int(float(raw_str))
    except Exception as exc:
        logger.warning("Fallback artist page scraping failed: %s", exc)

    return metrics


def scrape_spotify_artist(artist_id: str) -> Dict[str, Any]:
    """Coordinates scraping and normalization without any developer credentials."""
    token = fetch_anonymous_token(artist_id)

    monthly_listeners = 0
    followers = 0
    world_rank: Optional[int] = None
    top_cities: List[Dict[str, Any]] = []
    track_streams: List[int] = []

    graphql_success = False

    if token:
        overview_data = fetch_artist_overview_graphql(artist_id, token)
        if overview_data:
            artist_union = overview_data.get("artistUnion") or overview_data.get("artist") or {}
            stats = artist_union.get("stats", {})
            monthly_listeners = int(stats.get("monthlyListeners") or 0)
            followers = int(stats.get("followers") or 0)

            # World Rank
            raw_world_rank = stats.get("worldRank")
            if raw_world_rank is not None:
                try:
                    world_rank = int(raw_world_rank)
                except (ValueError, TypeError):
                    world_rank = None

            # Top Cities
            for c in stats.get("topCities", {}).get("items", []):
                city_name = c.get("city")
                if city_name:
                    top_cities.append({
                        "city": str(city_name),
                        "country": str(c.get("country") or ""),
                        "listeners": int(c.get("numberOfListeners") or 0)
                    })

            # Stream counts for top tracks
            top_tracks_obj = artist_union.get("discography", {}).get("topTracks", {}) or {}
            for item in top_tracks_obj.get("items", []):
                playcount = item.get("track", {}).get("playcount")
                if playcount is not None:
                    try:
                        track_streams.append(int(playcount))
                    except (ValueError, TypeError):
                        continue
            graphql_success = True

    # Fallback to HTML if GraphQL failed
    if not graphql_success or (monthly_listeners == 0 and followers == 0):
        fallback_stats = fetch_artist_page_fallback(artist_id)
        if monthly_listeners == 0:
            monthly_listeners = fallback_stats.get("monthly_listeners", 0)

    # Calculate stream aggregates safely
    top_1_streams = track_streams[0] if len(track_streams) >= 1 else 0
    top_10_cumulative = sum(track_streams[:10]) if track_streams else 0

    # Derived calculations
    stickiness_ratio = round((followers / monthly_listeners * 100), 2) if monthly_listeners > 0 else 0.0
    top_track_concentration = round((top_1_streams / top_10_cumulative * 100), 2) if top_10_cumulative > 0 else 0.0

    return {
        "artist_id": str(artist_id),
        "spotify_monthly_listeners": int(monthly_listeners),
        "spotify_followers": int(followers),
        "spotify_top_track_1_streams": int(top_1_streams),
        "spotify_top_10_cumulative_streams": int(top_10_cumulative),
        "spotify_world_rank": world_rank,
        "spotify_top_cities": top_cities,
        "stickiness_ratio_pct": stickiness_ratio,
        "top_track_concentration_pct": top_track_concentration
    }


def init_db(conn: sqlite3.Connection) -> None:
    """Ensures cache and historical tables exist."""
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS metrics_cache (
                channel_id TEXT PRIMARY KEY,
                data TEXT,
                updated_at TIMESTAMP
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS historical_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT,
                entity_id TEXT,
                data TEXT,
                extracted_at TIMESTAMP
            );
        """)


def save_to_cache(conn: sqlite3.Connection, artist_id: str, new_data: Dict[str, Any]) -> None:
    """Atomic save: Updates TTL in metrics_cache; logs to historical_metrics only on change."""
    now_iso = datetime.now(timezone.utc).isoformat()

    with conn:
        cursor = conn.cursor()

        cursor.execute("SELECT data FROM metrics_cache WHERE channel_id = ?", (artist_id,))
        row = cursor.fetchone()

        has_changed = False
        deltas = {}

        trackable_keys = [
            "spotify_monthly_listeners",
            "spotify_followers",
            "spotify_top_track_1_streams",
            "spotify_top_10_cumulative_streams"
        ]

        if row:
            old_data = json.loads(row[0])
            for key in trackable_keys:
                old_val = old_data.get(key) or 0
                new_val = new_data.get(key) or 0
                diff = new_val - old_val
                deltas[f"delta_{key}"] = diff
                if diff != 0:
                    has_changed = True
        else:
            has_changed = True
            for key in trackable_keys:
                deltas[f"delta_{key}"] = new_data.get(key) or 0

        historical_payload = {**new_data, "deltas": deltas}
        new_data_json = json.dumps(new_data, ensure_ascii=False)
        hist_data_json = json.dumps(historical_payload, ensure_ascii=False)

        # 1. Always UPSERT metrics_cache (Refreshes TTL)
        cursor.execute(
            """
            INSERT INTO metrics_cache (channel_id, data, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET
                data = excluded.data,
                updated_at = excluded.updated_at
            """,
            (artist_id, new_data_json, now_iso)
        )

        # 2. Only INSERT into historical_metrics on actual metric changes
        if has_changed:
            cursor.execute(
                """
                INSERT INTO historical_metrics (id, platform, entity_id, data, extracted_at)
                VALUES (NULL, 'spotify', ?, ?, ?)
                """,
                (artist_id, hist_data_json, now_iso)
            )
            logger.info("Recorded new historical entry with deltas for %s.", artist_id)
        else:
            logger.info("No metric changes detected for %s. Refreshed TTL without duplicate history.", artist_id)


def main():
    target_id = TARGET_ARTIST_ID
    if len(sys.argv) > 1:
        target_id = sys.argv[1]

    logger.info("Starting zero-cost Spotify metrics pipeline for ID: %s", target_id)

    # 1. Scrape & Normalize
    payload = scrape_spotify_artist(target_id)
    print("\n--- Extracted & Normalized Payload ---")
    print(json.dumps(payload, indent=2))

    # 2. Persist with Deduplication & Deltas
    try:
        conn = sqlite3.connect(DEFAULT_DB_PATH)
        init_db(conn)
        save_to_cache(conn, target_id, payload)
        conn.close()
        logger.info("Pipeline executed successfully.")
    except Exception as exc:
        logger.critical("Database pipeline error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()