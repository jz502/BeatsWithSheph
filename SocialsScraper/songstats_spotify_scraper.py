"""
songstats_spotify_scraper.py - Dedicated Scraper for Songstats Spotify Metrics

Extracts:
1. Current & Total Spotify Playlists & Reach (exact raw integers).
2. Free Spotify Popularity Index (0-100) & Global Rank.
3. Complete daily historical time-series (Playlists & Reach from 2021 to present).
4. Verified Cross-Platform links from JSON-LD schema.

Persists to socials_cache.db:
- metrics_cache: Namespaced snapshot with 24-hr TTL.
- historical_metrics: Deduplicated time-series log with deltas (platform='songstats_spotify').
- songstats_spotify_history: Normalized daily historical timeline table.
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

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_DB_PATH = "socials_cache.db"
PLATFORM_TAG = "songstats_spotify"

DEFAULT_SONGSTATS_ID = "y4ku2hlc"  # K.LAW
DEFAULT_ARTIST_SLUG = "k-law"
DEFAULT_SPOTIFY_ID = "3fNvLxWjqCfeHwBYtHmuGI"

ANALYTICS_TOP_URL = "https://data.songstats.com/api/v1/analytics/top"

# Headers from Songstats web client
SONGSTATS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:156.0) Gecko/20100101 Firefox/156.0",
    "Accept": "application/json",
    "Accept-Language": "en-AU,en-US;q=0.9,en;q=0.8",
    "Referer": "https://songstats.com/",
    "Origin": "https://songstats.com",
    "Content-Type": "application/json",
    "x-enigma": "3331363437323d363437303136323d",
    "fe-platform": "web",
    "fe-version": "143",
    "fe-service": "songstats",
    "supported-analytics-tab-ids": "tracks,tables,locations,insights,track_insights,posts,activities,recent_plays,recent_downloads,events,sync,revenue,followers,comments,retentions",
    "fe-build-timestamp": "1790433689"
}


def _get_session():
    """Initializes session with Chrome/Firefox TLS impersonation."""
    if HAS_CURL_CFFI:
        return cffi_requests.Session(impersonate="chrome124")
    session = cffi_requests.Session()
    session.headers.update(SONGSTATS_HEADERS)
    return session


def parse_abbreviated_number(val_str: str) -> int:
    """Converts strings like '123K' -> 123000, '3.72M' -> 3720000."""
    if not val_str:
        return 0
    clean = str(val_str).replace(",", "").replace("%", "").replace("#", "").replace("+", "").strip().upper()
    if clean.endswith("K"):
        return int(float(clean[:-1]) * 1_000)
    elif clean.endswith("M"):
        return int(float(clean[:-1]) * 1_000_000)
    elif clean.endswith("B"):
        return int(float(clean[:-1]) * 1_000_000_000)
    try:
        return int(float(clean))
    except (ValueError, TypeError):
        return 0


# ==========================================
# 1. SCRAPE HTML METADATA & LINKS
# ==========================================

def scrape_songstats_metadata(songstats_id: str, slug: str) -> Dict[str, Any]:
    """Scrapes the server-side JSON-LD metadata for verified Spotify ID, links and bio."""
    url = f"https://songstats.com/artist/{songstats_id}/{slug}?source=spotify"
    session = _get_session()
    logger.info("Scraping metadata from HTML page: %s", url)

    data_payload: Dict[str, Any] = {
        "songstats_artist_id": songstats_id,
        "spotify_artist_id": DEFAULT_SPOTIFY_ID,
        "cross_platform_links": {},
        "artist_bio": ""
    }

    try:
        resp = session.get(url, headers=SONGSTATS_HEADERS, timeout=15)
        if resp.status_code == 200:
            json_ld_blocks = re.findall(r'<script type="application/ld\+json">(.*?)</script>', resp.text, re.DOTALL)
            for block in json_ld_blocks:
                try:
                    parsed_json = json.loads(block)
                    for item in parsed_json.get("@graph", []):
                        if item.get("@type") == "MusicGroup":
                            data_payload["artist_bio"] = item.get("description", "")
                            for link in item.get("sameAs", []):
                                if "open.spotify.com/artist/" in link:
                                    extracted_id = link.rstrip("/").split("/")[-1].split("?")[0]
                                    data_payload["spotify_artist_id"] = extracted_id
                                    data_payload["cross_platform_links"]["spotify"] = link
                                elif "music.apple.com" in link:
                                    data_payload["cross_platform_links"]["apple_music"] = link
                                elif "instagram.com" in link:
                                    data_payload["cross_platform_links"]["instagram"] = link
                                elif "soundcloud.com" in link:
                                    data_payload["cross_platform_links"]["soundcloud"] = link
                                elif "deezer.com" in link:
                                    data_payload["cross_platform_links"]["deezer"] = link
                                elif "beatport.com" in link:
                                    data_payload["cross_platform_links"]["beatport"] = link
                except json.JSONDecodeError:
                    continue
    except Exception as exc:
        logger.warning("Metadata scraping error: %s", exc)

    return data_payload


# ==========================================
# 2. QUERY THE ANALYTICS API ENDPOINT
# ==========================================

def fetch_songstats_analytics_top(songstats_id: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Hits Songstats' /api/v1/analytics/top endpoint to fetch:
    - Current and total metrics from iconData.
    - Full merged time-series history from seriesData.
    """
    session = _get_session()
    params = {
        "idUnique": songstats_id,
        "source": "spotify",
        "isMobileWeb": "false"
    }

    metrics = {
        "songstats_spotify_playlists_current": 0,
        "songstats_spotify_playlist_reach_current": 0,
        "songstats_spotify_playlists_total": 0,
        "songstats_spotify_playlist_reach_total": 0,
        "songstats_spotify_popularity": None,
        "songstats_spotify_global_rank": None
    }
    history_records: List[Dict[str, Any]] = []

    try:
        logger.info("Querying Songstats Analytics API: %s", ANALYTICS_TOP_URL)
        resp = session.get(ANALYTICS_TOP_URL, params=params, headers=SONGSTATS_HEADERS, timeout=15)
        
        if resp.status_code != 200:
            logger.error("Analytics endpoint returned HTTP %d", resp.status_code)
            return metrics, history_records

        data = resp.json()
        if data.get("result") != "success":
            logger.warning("API returned unexpected response: %s", data.get("message"))
            return metrics, history_records

        chart = data.get("chart", {})

        # --- A. Parse iconData (Current & Total Metrics) ---
        icon_data = chart.get("iconData", [])
        for item in icon_data:
            text = item.get("text", "")
            secondary = item.get("secondaryText", "")
            count_str = str(item.get("count", "")).strip()

            if text == "Playlists" and secondary == "current":
                metrics["songstats_spotify_playlists_current"] = parse_abbreviated_number(count_str)
            elif text == "Playlist Reach" and secondary == "current":
                metrics["songstats_spotify_playlist_reach_current"] = parse_abbreviated_number(count_str)
            elif text == "Playlists" and secondary == "total":
                metrics["songstats_spotify_playlists_total"] = parse_abbreviated_number(count_str)
            elif text == "Playlist Reach" and secondary == "total":
                metrics["songstats_spotify_playlist_reach_total"] = parse_abbreviated_number(count_str)
            elif text == "Popularity" and secondary == "current":
                # E.g. "6%" -> int(6)
                metrics["songstats_spotify_popularity"] = parse_abbreviated_number(count_str)
            elif text == "Artist Rank" and secondary == "global":
                metrics["songstats_spotify_global_rank"] = count_str

        # --- B. Parse seriesData (Full Daily History) ---
        series_data = chart.get("seriesData", [])
        playlists_series = {}
        reach_series = {}

        for s in series_data:
            name = s.get("name")
            data_points = s.get("data", [])
            if name == "Playlists":
                playlists_series = {point[0]: point[1] for point in data_points if len(point) == 2}
            elif name == "Playlist Reach":
                reach_series = {point[0]: point[1] for point in data_points if len(point) == 2}

        # Merge series by timestamp
        all_timestamps = sorted(set(playlists_series.keys()) | set(reach_series.keys()))
        for ts in all_timestamps:
            date_str = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
            p_val = int(playlists_series.get(ts, 0))
            r_val = int(reach_series.get(ts, 0))
            history_records.append({
                "date": date_str,
                "playlists": p_val,
                "playlist_reach": r_val
            })

        # Overwrite current reach with the exact raw integer from the latest history point
        if history_records:
            latest = history_records[-1]
            if latest["playlists"] > 0:
                metrics["songstats_spotify_playlists_current"] = latest["playlists"]
            if latest["playlist_reach"] > 0:
                metrics["songstats_spotify_playlist_reach_current"] = latest["playlist_reach"]

        logger.info("Extracted %d historical daily points successfully!", len(history_records))

    except Exception as exc:
        logger.error("Error fetching analytics top data: %s", exc)

    return metrics, history_records


# ==========================================
# 3. DATABASE PERSISTENCE
# ==========================================

def init_db(conn: sqlite3.Connection) -> None:
    """Ensures cache, historical, and specialized Songstats history tables exist."""
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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS songstats_spotify_history (
                entity_id TEXT,
                record_date DATE,
                playlists INTEGER,
                playlist_reach INTEGER,
                PRIMARY KEY (entity_id, record_date)
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_songstats_spotify_history_date 
            ON songstats_spotify_history(entity_id, record_date);
        """)


def save_songstats_spotify_data(
    conn: sqlite3.Connection,
    spotify_id: str,
    current_data: Dict[str, Any],
    history_data: List[Dict[str, Any]]
) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    cache_key = f"{PLATFORM_TAG}:{spotify_id}"

    with conn:
        cursor = conn.cursor()

        # 1. Delta Calculation against previous cache
        cursor.execute("SELECT data FROM metrics_cache WHERE channel_id = ?", (cache_key,))
        row = cursor.fetchone()

        has_changed = False
        deltas = {}
        tracked_keys = [
            "songstats_spotify_playlists_current",
            "songstats_spotify_playlist_reach_current",
            "songstats_spotify_popularity"
        ]

        if row:
            old_data = json.loads(row[0])
            for key in tracked_keys:
                old_val = old_data.get(key) or 0
                new_val = current_data.get(key) or 0
                diff = new_val - old_val
                deltas[f"delta_{key}"] = diff
                if diff != 0:
                    has_changed = True
        else:
            has_changed = True
            for key in tracked_keys:
                deltas[f"delta_{key}"] = current_data.get(key) or 0

        historical_payload = {**current_data, "deltas": deltas}
        current_data_json = json.dumps(current_data, ensure_ascii=False)
        hist_data_json = json.dumps(historical_payload, ensure_ascii=False)

        # 2. Always UPSERT into metrics_cache (Maintains TTL)
        cursor.execute(
            """
            INSERT INTO metrics_cache (channel_id, data, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET
                data = excluded.data,
                updated_at = excluded.updated_at
            """,
            (cache_key, current_data_json, now_iso)
        )

        # 3. Conditional INSERT into historical_metrics on change
        if has_changed:
            cursor.execute(
                """
                INSERT INTO historical_metrics (id, platform, entity_id, data, extracted_at)
                VALUES (NULL, ?, ?, ?, ?)
                """,
                (PLATFORM_TAG, spotify_id, hist_data_json, now_iso)
            )
            logger.info("Recorded new historical entry with deltas for %s.", cache_key)
        else:
            logger.info("No metric changes detected for %s. Refreshed TTL without duplicate history.", cache_key)

        # 4. Bulk INSERT OR IGNORE into songstats_spotify_history (Backfill 2021-2026)
        if history_data:
            records = [
                (spotify_id, entry["date"], entry["playlists"], entry["playlist_reach"])
                for entry in history_data
            ]
            cursor.executemany(
                """
                INSERT OR IGNORE INTO songstats_spotify_history
                (entity_id, record_date, playlists, playlist_reach)
                VALUES (?, ?, ?, ?)
                """,
                records
            )
            logger.info("Synced %d timeline records into songstats_spotify_history.", len(records))


# ==========================================
# 4. MAIN ENTRY POINT
# ==========================================

def main():
    songstats_id = DEFAULT_SONGSTATS_ID
    slug = DEFAULT_ARTIST_SLUG

    if len(sys.argv) > 1:
        songstats_id = sys.argv[1]
    if len(sys.argv) > 2:
        slug = sys.argv[2]

    logger.info("Starting Songstats Spotify pipeline for ID: %s (%s)", songstats_id, slug)

    # 1. Scrape metadata & cross-platform links
    meta = scrape_songstats_metadata(songstats_id, slug)
    spotify_id = meta.get("spotify_artist_id") or DEFAULT_SPOTIFY_ID

    # 2. Query Analytics API for current metrics and full history
    analytics_metrics, history_records = fetch_songstats_analytics_top(songstats_id)

    combined_payload = {**meta, **analytics_metrics}
    print("\n--- Extracted Songstats Spotify Payload ---")
    print(json.dumps(combined_payload, indent=2))

    if history_records:
        print(f"\n--- Retrieved {len(history_records)} Daily History Records (First 2 & Last 2) ---")
        print(json.dumps(history_records[:2] + history_records[-2:], indent=2))

    # 3. Persist to SQLite
    try:
        conn = sqlite3.connect(DEFAULT_DB_PATH)
        init_db(conn)
        save_songstats_spotify_data(conn, spotify_id, combined_payload, history_records)
        conn.close()
        logger.info("Songstats Spotify pipeline completed successfully.")
    except Exception as exc:
        logger.critical("Database pipeline error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()