"""
spotify_scraper.py - Local Zero-Cost Spotify Public Metrics Scraper

Extracts public artist metrics (Monthly Listeners, Followers, Top Track Streams)
from Spotify's web player infrastructure without an authenticated API app token.
Persists normalized metrics into SQLite (socials_cache.db) with 24-hr TTL support.
"""

import json
import logging
import re
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# Attempt import of curl_cffi for TLS fingerprint impersonation; fall back to requests
try:
    from curl_cffi import requests as cffi_requests
    HAS_CURL_CFFI = True
except ImportError:
    import requests as cffi_requests
    HAS_CURL_CFFI = False

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Constants & Configuration
DEFAULT_DB_PATH = "socials_cache.db"
TARGET_ARTIST_ID = "3fNvLxWjqCfeHwBYtHmuGI"  # K.LAW (Joshua Kalaw)

PATHFINDER_URL = "https://api-partner.spotify.com/pathfinder/v1/query"

# Persisted query hashes for queryArtistOverview known to Spotify's web client
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
    """Initializes a requests or curl_cffi session with browser emulation."""
    if HAS_CURL_CFFI:
        return cffi_requests.Session(impersonate="chrome124")
    session = cffi_requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


def fetch_anonymous_token_and_embed_data(artist_id: str) -> Tuple[Optional[str], Optional[dict]]:
    """
    Scrapes the Spotify Embed page for the artist.
    Extracts the anonymous Bearer token and fallback SSR hydration entity data.
    """
    embed_url = f"https://open.spotify.com/embed/artist/{artist_id}"
    session = _get_session()

    try:
        logger.info("Harvesting anonymous token from embed page: %s", embed_url)
        response = session.get(embed_url, timeout=12)
        if response.status_code != 200:
            logger.warning("Embed page returned HTTP %d", response.status_code)
            return None, None

        html = response.text
        match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if not match:
            logger.warning("Could not locate __NEXT_DATA__ in embed page HTML.")
            return None, None

        data = json.loads(match.group(1))
        page_props = data.get("props", {}).get("pageProps", {})

        # Extract anonymous accessToken from state settings session
        token = (
            page_props.get("state", {})
            .get("settings", {})
            .get("session", {})
            .get("accessToken")
        )

        # In case the JSON structure slightly shifts, search recursively
        if not token:
            def find_key(obj, target):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if k == target and isinstance(v, str):
                            return v
                        res = find_key(v, target)
                        if res:
                            return res
                elif isinstance(obj, list):
                    for item in obj:
                        res = find_key(item, target)
                        if res:
                            return res
                return None
            token = find_key(data, "accessToken")

        embed_entity = page_props.get("state", {}).get("data", {}).get("entity")
        return token, embed_entity

    except Exception as exc:
        logger.error("Error retrieving anonymous token: %s", exc)
        return None, None


def fetch_artist_overview_graphql(artist_id: str, access_token: str) -> Optional[dict]:
    """
    Queries Spotify's Pathfinder GraphQL API using the anonymous Bearer token.
    """
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
                    logger.info("Successfully fetched GraphQL overview with hash %s...", sha256_hash[:8])
                    return result["data"]
            elif resp.status_code == 400:
                # Hash mismatch; continue to next hash
                continue
            else:
                logger.warning("GraphQL query returned HTTP %d with hash %s...", resp.status_code, sha256_hash[:8])
        except Exception as exc:
            logger.warning("GraphQL request error with hash %s: %s", sha256_hash[:8], exc)

    return None


def fetch_artist_page_fallback(artist_id: str) -> Dict[str, int]:
    """
    Scrapes the public artist HTML profile to extract monthly listeners
    from Open Graph or meta descriptions if GraphQL is unreachable.
    """
    artist_url = f"https://open.spotify.com/artist/{artist_id}"
    session = _get_session()
    metrics = {"monthly_listeners": 0, "followers": 0}

    try:
        resp = session.get(artist_url, timeout=12)
        if resp.status_code == 200:
            html = resp.text
            # Look for patterns like "15,200 monthly listeners" or "1.5M monthly listeners"
            match = re.search(r'([\d,\.]+[KkMmBb]?)\s+monthly\s+listeners', html, re.IGNORECASE)
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
        logger.error("Fallback artist page scraping failed: %s", exc)

    return metrics


def scrape_spotify_artist(artist_id: str) -> Dict[str, Any]:
    """
    Task 1 & Task 2: Coordinates extraction, parsing, and normalization
    into the required JSON schema.
    """
    token, embed_entity = fetch_anonymous_token_and_embed_data(artist_id)

    monthly_listeners = 0
    followers = 0
    track_streams: List[int] = []

    graphql_success = False

    if token:
        overview_data = fetch_artist_overview_graphql(artist_id, token)
        if overview_data:
            artist_union = (
                overview_data.get("artistUnion")
                or overview_data.get("artist")
                or {}
            )
            stats = artist_union.get("stats", {})
            monthly_listeners = int(stats.get("monthlyListeners") or 0)
            followers = int(stats.get("followers") or 0)

            # Extract track play counts
            top_tracks_obj = (
                artist_union.get("discography", {}).get("topTracks", {})
                or {}
            )
            track_items = top_tracks_obj.get("items", [])

            for item in track_items:
                track = item.get("track", {})
                playcount_val = track.get("playcount")
                if playcount_val is not None:
                    try:
                        track_streams.append(int(playcount_val))
                    except (ValueError, TypeError):
                        continue
            graphql_success = True

    # Fallback to embed data or main HTML if GraphQL failed or returned empty
    if not graphql_success or (monthly_listeners == 0 and followers == 0):
        logger.info("Applying fallback parsing from HTML/embed entities...")
        fallback_stats = fetch_artist_page_fallback(artist_id)
        if monthly_listeners == 0:
            monthly_listeners = fallback_stats.get("monthly_listeners", 0)

    # If stream counts could not be retrieved from GraphQL, ensure safe handling
    top_1_streams = track_streams[0] if len(track_streams) >= 1 else 0
    top_10_cumulative = sum(track_streams[:10]) if track_streams else 0

    # Normalization payload according to expected schema
    payload = {
        "artist_id": str(artist_id),
        "spotify_monthly_listeners": int(monthly_listeners),
        "spotify_followers": int(followers),
        "spotify_top_track_1_streams": int(top_1_streams),
        "spotify_top_10_cumulative_streams": int(top_10_cumulative)
    }

    return payload


def init_db(conn: sqlite3.Connection) -> None:
    """Ensures cache and historical tables exist before running queries."""
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


def save_to_cache(conn: sqlite3.Connection, artist_id: str, data: Dict[str, Any]) -> None:
    """
    Task 3: Executes two actions atomically in a single transaction:
    1. UPSERT into metrics_cache table.
    2. INSERT into historical_metrics table.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    json_data = json.dumps(data, ensure_ascii=False)

    try:
        with conn:  # Context manager starts transaction and commits upon clean exit
            cursor = conn.cursor()

            # 1. UPSERT into metrics_cache
            cursor.execute(
                """
                INSERT INTO metrics_cache (channel_id, data, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    data = excluded.data,
                    updated_at = excluded.updated_at
                """,
                (artist_id, json_data, now_iso)
            )

            # 2. INSERT into historical_metrics
            cursor.execute(
                """
                INSERT INTO historical_metrics (id, platform, entity_id, data, extracted_at)
                VALUES (NULL, 'spotify', ?, ?, ?)
                """,
                (artist_id, json_data, now_iso)
            )

        logger.info("Successfully persisted metrics for artist %s to database.", artist_id)
    except Exception as exc:
        logger.error("Failed to save metrics to database: %s", exc)
        raise


def main():
    target_id = TARGET_ARTIST_ID
    if len(sys.argv) > 1:
        target_id = sys.argv[1]

    logger.info("Starting Spotify public metrics scraper for ID: %s", target_id)

    # 1. Scrape and Normalize
    normalized_data = scrape_spotify_artist(target_id)
    print("\n--- Extracted & Normalized Payload ---")
    print(json.dumps(normalized_data, indent=2))

    # 2. Persist to SQLite
    try:
        conn = sqlite3.connect(DEFAULT_DB_PATH)
        init_db(conn)
        save_to_cache(conn, target_id, normalized_data)
        conn.close()
        logger.info("Pipeline executed successfully.")
    except Exception as e:
        logger.critical("Database pipeline error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()