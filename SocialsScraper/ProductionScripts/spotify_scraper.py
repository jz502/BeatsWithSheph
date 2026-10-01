#!/usr/bin/env python3
"""
spotify_scraper.py - Production Zero-Cost Local Spotify Analytics Scraper

Container-ready worker designed for execution by a master orchestrator.
- Zero-cost: Uses anonymous web player token harvesting & Pathfinder GraphQL.
- Resilient: Chrome TLS fingerprint impersonation via curl_cffi.
- Strict Error Handling: Fails loudly with exit code 1 on non-200 or data anomalies.
- Storage: Docker volume persistent SQLite at data/socials_cache.db with WAL mode.
- Deduplication: Namespaced TTL metrics_cache + delta-driven historical_metrics.
"""

import argparse
import json
import logging
import os
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

# Production Logger Configuration (Routes to sys.stdout for Docker/Portainer log aggregator)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("spotify_scraper")

# Rule 1: Docker persistent volume database path
DB_PATH = "data/socials_cache.db"

# Internal Spotify Web Player API Constants
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
    """Initializes session with Chrome TLS fingerprinting to bypass anti-bot challenges."""
    if HAS_CURL_CFFI:
        return cffi_requests.Session(impersonate="chrome124")
    session = cffi_requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


def fetch_anonymous_token(artist_id: str) -> str:
    """
    Harvests an ephemeral anonymous Web Player token from the artist's embed page.
    Rule 3: Raises RuntimeError if network times out, returns non-200, or payload is missing.
    """
    embed_url = f"https://open.spotify.com/embed/artist/{artist_id}"
    session = _get_session()

    try:
        response = session.get(embed_url, timeout=15)
    except Exception as exc:
        raise RuntimeError(f"Network error accessing Spotify embed endpoint: {exc}") from exc

    if response.status_code != 200:
        raise RuntimeError(
            f"Spotify embed returned HTTP {response.status_code} for artist '{artist_id}'"
        )

    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', response.text, re.DOTALL)
    if not match:
        raise RuntimeError(f"Could not locate __NEXT_DATA__ state container for artist '{artist_id}'")

    try:
        payload = json.loads(match.group(1))
    except Exception as exc:
        raise RuntimeError(f"Corrupt JSON payload in __NEXT_DATA__ for '{artist_id}': {exc}") from exc

    token = (
        payload.get("props", {})
        .get("pageProps", {})
        .get("state", {})
        .get("settings", {})
        .get("session", {})
        .get("accessToken")
    )

    if not token:
        # Fallback recursive search if Spotify's pageProps nesting shifts
        def find_key(obj: Any, target: str) -> Optional[str]:
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
        token = find_key(payload, "accessToken")

    if not token:
        raise RuntimeError(f"Failed to harvest anonymous accessToken for artist '{artist_id}'")

    return token


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
            resp = session.get(PATHFINDER_URL, params=params, headers=headers, timeout=15)
            if resp.status_code == 200:
                result = resp.json()
                if "data" in result and result["data"]:
                    return result["data"]
            elif resp.status_code in (400, 404):
                # Hash signature rotated, continue to next known hash
                continue
            else:
                logger.warning(
                    "GraphQL hash %s returned unexpected HTTP %d",
                    sha256_hash[:8], resp.status_code
                )
        except Exception as exc:
            logger.warning("GraphQL request failed for hash %s: %s", sha256_hash[:8], exc)
            continue

    return None


def fetch_artist_page_fallback(artist_id: str) -> int:
    """Fallback: Regex parsing from public HTML profile if GraphQL hashes rotate."""
    artist_url = f"https://open.spotify.com/artist/{artist_id}"
    session = _get_session()

    try:
        resp = session.get(artist_url, timeout=15)
    except Exception as exc:
        raise RuntimeError(f"Fallback HTTP request failed for artist '{artist_id}': {exc}") from exc

    if resp.status_code != 200:
        raise RuntimeError(f"Fallback artist URL returned HTTP {resp.status_code} for '{artist_id}'")

    match = re.search(r'([\d,\.]+[KkMmBb]?)\s+monthly\s+listeners', resp.text, re.IGNORECASE)
    if not match:
        raise RuntimeError(f"Fallback regex could not locate monthly listeners on artist page '{artist_id}'")

    raw_str = match.group(1).replace(",", "").strip()
    if raw_str.upper().endswith("K"):
        return int(float(raw_str[:-1]) * 1_000)
    elif raw_str.upper().endswith("M"):
        return int(float(raw_str[:-1]) * 1_000_000)
    elif raw_str.upper().endswith("B"):
        return int(float(raw_str[:-1]) * 1_000_000_000)
    return int(float(raw_str))


def scrape_spotify_artist(artist_id: str) -> Dict[str, Any]:
    """
    Coordinates extraction and metric calculations.
    Rule 3: Fails loudly if Spotify metrics cannot be parsed.
    """
    token = fetch_anonymous_token(artist_id)

    monthly_listeners = 0
    followers = 0
    world_rank: Optional[int] = None
    top_cities: List[Dict[str, Any]] = []
    track_streams: List[int] = []

    graphql_data = fetch_artist_overview_graphql(artist_id, token)

    if graphql_data:
        artist_union = graphql_data.get("artistUnion") or graphql_data.get("artist") or {}
        stats = artist_union.get("stats", {})

        monthly_listeners = int(stats.get("monthlyListeners") or 0)
        followers = int(stats.get("followers") or 0)

        raw_world_rank = stats.get("worldRank")
        if raw_world_rank is not None:
            try:
                world_rank = int(raw_world_rank)
            except (ValueError, TypeError):
                world_rank = None

        for c in stats.get("topCities", {}).get("items", []):
            city_name = c.get("city")
            if city_name:
                top_cities.append({
                    "city": str(city_name),
                    "country": str(c.get("country") or ""),
                    "listeners": int(c.get("numberOfListeners") or 0)
                })

        top_tracks_obj = artist_union.get("discography", {}).get("topTracks", {}) or {}
        for item in top_tracks_obj.get("items", []):
            playcount = item.get("track", {}).get("playcount")
            if playcount is not None:
                try:
                    track_streams.append(int(playcount))
                except (ValueError, TypeError):
                    continue

    # If GraphQL was blocked or returned empty metrics, attempt page fallback
    if monthly_listeners == 0 and followers == 0:
        logger.warning("GraphQL yielded 0 metrics. Attempting direct HTML fallback...")
        monthly_listeners = fetch_artist_page_fallback(artist_id)

    # Rule 3 Enforcement: Never return dummy/empty payloads on critical failure
    if monthly_listeners == 0 and followers == 0 and not track_streams:
        raise RuntimeError(
            f"Extraction failed: Received 0 metrics across all scrapers for artist '{artist_id}'. "
            "Profile may be invalid or Spotify has updated its web player data layer."
        )

    # Safe stream calculations (handles artists with < 10 tracks without crashing)
    top_1_streams = track_streams[0] if len(track_streams) >= 1 else 0
    top_10_cumulative = sum(track_streams[:10]) if track_streams else 0

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
    """
    Rule 5 & Task 1: Initializes tables with matching topology.
    Enforces SQLite WAL mode for high-concurrency production orchestration.
    """
    with conn:
        conn.execute("PRAGMA journal_mode = WAL;")
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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS artist_identities (
                songstats_id TEXT PRIMARY KEY,
                artist_name TEXT,
                spotify_id TEXT,
                youtube_id TEXT,
                instagram_handle TEXT,
                soundcloud_handle TEXT,
                updated_at TIMESTAMP
            );
        """)


def save_to_cache(conn: sqlite3.Connection, artist_id: str, current_data: Dict[str, Any]) -> None:
    """
    Rule 5 & Task 2: Atomic database persistence.
    1. Namespaces cache_key: 'spotify:{artist_id}'
    2. Compares numeric metrics against previous snapshot to calculate integer deltas.
    3. Always UPSERTs metrics_cache (refreshes 24-hr TTL).
    4. Conditionally INSERTs into historical_metrics only if metrics have changed (or first run).
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    cache_key = f"spotify:{artist_id}"

    trackable_keys = [
        "spotify_monthly_listeners",
        "spotify_followers",
        "spotify_top_track_1_streams",
        "spotify_top_10_cumulative_streams"
    ]

    with conn:
        cursor = conn.cursor()

        # Step 1: Query metrics_cache for previous snapshot
        cursor.execute("SELECT data FROM metrics_cache WHERE channel_id = ?", (cache_key,))
        row = cursor.fetchone()

        has_changed = False
        deltas: Dict[str, int] = {}

        if row:
            try:
                old_data = json.loads(row[0])
            except Exception:
                old_data = {}

            for key in trackable_keys:
                old_val = int(old_data.get(key) or 0)
                new_val = int(current_data.get(key) or 0)
                diff = new_val - old_val
                deltas[f"delta_{key}"] = diff
                if diff != 0:
                    has_changed = True
        else:
            # First run: Everything is a net-positive change
            has_changed = True
            for key in trackable_keys:
                deltas[f"delta_{key}"] = int(current_data.get(key) or 0)

        # Prepare payloads
        cache_json = json.dumps(current_data, ensure_ascii=False)
        historical_payload = {**current_data, "deltas": deltas}
        hist_json = json.dumps(historical_payload, ensure_ascii=False)

        # Step 2: Always UPSERT metrics_cache (refreshes TTL)
        cursor.execute(
            """
            INSERT INTO metrics_cache (channel_id, data, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET
                data = excluded.data,
                updated_at = excluded.updated_at
            """,
            (cache_key, cache_json, now_iso)
        )

        # Step 3: Conditional historical insert to prevent ledger bloat
        if has_changed:
            cursor.execute(
                """
                INSERT INTO historical_metrics (id, platform, entity_id, data, extracted_at)
                VALUES (NULL, 'spotify', ?, ?, ?)
                """,
                (artist_id, hist_json, now_iso)
            )
            logger.info("Changes detected. Appended new historical ledger row for %s.", cache_key)
        else:
            logger.info("No metric changes detected for %s. Refreshed TTL; skipped historical row.", cache_key)


def main():
    # Rule 2: Strict argument handling (no hardcoded defaults)
    parser = argparse.ArgumentParser(
        description="Production Spotify Public Metrics Scraper for Music Analytics Pipeline"
    )
    parser.add_argument(
        "artist_id",
        type=str,
        help="Spotify Artist ID to scrape (e.g., '3fNvLxWjqCfeHwBYtHmuGI')"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=DB_PATH,
        help=f"Path to SQLite database (default: {DB_PATH})"
    )

    args = parser.parse_args()
    target_id = args.artist_id.strip()
    db_target = args.db_path

    logger.info("Initiating scrape worker for Spotify artist: %s", target_id)

    # Rule 1 Check: Ensure directory for SQLite volume exists
    db_dir = os.path.dirname(db_target)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    # Execute extraction (Rule 3 & 4: Fail loudly on errors, logging to stdout)
    try:
        extracted_data = scrape_spotify_artist(target_id)
        logger.info("Extracted payload: %s", json.dumps(extracted_data))
    except Exception as exc:
        logger.error("Scraping execution halted with error: %s", exc)
        sys.exit(1)

    # Persist metrics to SQLite
    try:
        conn = sqlite3.connect(db_target)
        init_db(conn)
        save_to_cache(conn, target_id, extracted_data)
        conn.close()
        logger.info("Scraper execution finalized successfully for artist %s.", target_id)
    except Exception as exc:
        logger.error("Database persistence transaction failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()