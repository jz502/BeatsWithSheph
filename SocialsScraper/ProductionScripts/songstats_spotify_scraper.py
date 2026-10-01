#!/usr/bin/env python3
"""
songstats_spotify_scraper.py
----------------------------
Production Dedicated Scraper for Songstats Spotify Metrics in Containerized Environments.

Extracts:
1. Current & Total Spotify Playlists & Reach (exact raw integers).
2. Free Spotify Popularity Index (0-100) & Global Rank.
3. Complete daily historical time-series (Playlists & Reach from 2021 to present).
4. Verified Cross-Platform links from JSON-LD schema.

Persists to data/socials_cache.db:
- metrics_cache: Namespaced snapshot with 24-hr TTL.
- historical_metrics: Deduplicated time-series log with deltas (platform='songstats_spotify').
- songstats_spotify_history: Normalized daily historical timeline table.
"""

import argparse
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import re
import sqlite3
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

try:
    from curl_cffi import requests as cffi_requests
    HAS_CURL_CFFI = True
except ImportError:
    import requests as cffi_requests
    HAS_CURL_CFFI = False

# =====================================================================
# Rule 1 & Rule 4: Docker Configuration & Logging Setup
# =====================================================================

DB_PATH = "data/socials_cache.db"
PLATFORM_TAG = "songstats_spotify"
ANALYTICS_TOP_URL = "https://data.songstats.com/api/v1/analytics/top"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("songstats_spotify_scraper")

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


# =====================================================================
# Utilities
# =====================================================================

def parse_abbreviated_number(val_str: Any) -> int:
    """Converts strings like '123K' -> 123000, '3.72M' -> 3720000."""
    if val_str is None:
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


def parse_artist_input(artist_input: str, explicit_slug: Optional[str] = None) -> Tuple[str, str]:
    """
    Extracts the Songstats ID and slug from a URL or bare identifier.
    Example: 'https://songstats.com/artist/y4ku2hlc/k-law' -> ('y4ku2hlc', 'k-law')
    """
    cleaned = artist_input.strip()
    match = re.search(r"songstats\.com/artist/([a-zA-Z0-9]+)(?:/([a-zA-Z0-9_-]+))?", cleaned)
    if match:
        songstats_id = match.group(1)
        slug = explicit_slug or match.group(2) or "_"
        return songstats_id, slug

    tokens = [t for t in cleaned.strip("/").split("/") if t]
    songstats_id = tokens[-1] if tokens else cleaned
    slug = explicit_slug or "_"
    return songstats_id, slug


def _get_session():
    """Initializes session with Chrome TLS impersonation."""
    if HAS_CURL_CFFI:
        return cffi_requests.Session(impersonate="chrome124")
    session = cffi_requests.Session()
    session.headers.update(SONGSTATS_HEADERS)
    return session


# =====================================================================
# Rule 3: Fail-Loud Metadata & API Extraction
# =====================================================================

def scrape_songstats_metadata(songstats_id: str, slug: str) -> Dict[str, Any]:
    """
    Scrapes the server-side JSON-LD metadata for verified Spotify ID, links, and bio.
    Fails loudly if the artist page cannot be resolved or is blocked.
    """
    url = f"https://songstats.com/artist/{songstats_id}/{slug}?source=spotify" if slug != "_" else f"https://songstats.com/artist/{songstats_id}?source=spotify"
    session = _get_session()
    logger.info("Scraping metadata from HTML page: %s", url)

    try:
        resp = session.get(url, headers=SONGSTATS_HEADERS, timeout=20)
    except Exception as exc:
        logger.critical("Fatal: Network failure accessing Songstats artist page (%s): %s", url, exc, exc_info=True)
        sys.exit(1)

    if resp.status_code >= 400:
        logger.critical("Fatal: Artist HTML page returned non-200 HTTP %d for %s", resp.status_code, url)
        sys.exit(1)

    data_payload: Dict[str, Any] = {
        "songstats_artist_id": songstats_id,
        "spotify_artist_id": None,
        "cross_platform_links": {},
        "artist_bio": ""
    }

    json_ld_blocks = re.findall(r'<script type="application/ld\+json">(.*?)</script>', resp.text, re.DOTALL)
    if not json_ld_blocks:
        logger.critical("Fatal: No JSON-LD metadata block located on artist page: %s", url)
        sys.exit(1)

    for block in json_ld_blocks:
        try:
            parsed_json = json.loads(block)
            graph_items = parsed_json.get("@graph", []) if isinstance(parsed_json, dict) else (parsed_json if isinstance(parsed_json, list) else [])
            for item in graph_items:
                if isinstance(item, dict) and item.get("@type") == "MusicGroup":
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

    if not data_payload["spotify_artist_id"]:
        logger.critical("Fatal: Unable to resolve verified Spotify Artist ID from JSON-LD schema for: %s", songstats_id)
        sys.exit(1)

    logger.info("Resolved verified Spotify Artist ID: %s", data_payload["spotify_artist_id"])
    return data_payload


def fetch_songstats_analytics_top(songstats_id: str, slug: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Queries Songstats' /api/v1/analytics/top endpoint.
    Fails loudly if the endpoint returns non-200 or an error payload.
    """
    session = _get_session()

    # Pre-flight warmup: visit the artist web page to initialize session state & cookies
    warmup_url = f"https://songstats.com/artist/{songstats_id}/{slug}?source=spotify" if slug != "_" else f"https://songstats.com/artist/{songstats_id}?source=spotify"
    try:
        session.get(warmup_url, headers=SONGSTATS_HEADERS, timeout=20)
        time.sleep(1.0)
    except Exception as exc:
        logger.debug("Session warmup notice: %s", exc)

    params = {
        "idUnique": songstats_id,
        "source": "spotify",
        "isMobileWeb": "false"
    }

    metrics: Dict[str, Any] = {
        "songstats_spotify_playlists_current": 0,
        "songstats_spotify_playlist_reach_current": 0,
        "songstats_spotify_playlists_total": 0,
        "songstats_spotify_playlist_reach_total": 0,
        "songstats_spotify_popularity": None,
        "songstats_spotify_global_rank": None
    }
    history_records: List[Dict[str, Any]] = []

    logger.info("Querying Songstats Analytics API: %s?idUnique=%s", ANALYTICS_TOP_URL, songstats_id)
    try:
        resp = session.get(ANALYTICS_TOP_URL, params=params, headers=SONGSTATS_HEADERS, timeout=25)
    except Exception as exc:
        logger.critical("Fatal: Network error contacting Songstats Analytics API: %s", exc, exc_info=True)
        sys.exit(1)

    if resp.status_code >= 400:
        logger.critical("Fatal: Songstats Analytics API returned HTTP %d for artist ID %s", resp.status_code, songstats_id)
        sys.exit(1)

    try:
        data = resp.json()
    except Exception as exc:
        logger.critical("Fatal: Invalid JSON response from Analytics API: %s", exc)
        sys.exit(1)

    if data.get("result") != "success":
        logger.critical("Fatal: API returned unfulfilled response: %s", data.get("message"))
        sys.exit(1)

    chart = data.get("chart")
    if not chart or not isinstance(chart, dict):
        logger.critical("Fatal: Expected 'chart' object missing from Analytics API response.")
        sys.exit(1)

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
            metrics["songstats_spotify_popularity"] = parse_abbreviated_number(count_str)
        elif text == "Artist Rank" and secondary == "global":
            metrics["songstats_spotify_global_rank"] = count_str

    # --- B. Parse seriesData (Full Daily History) ---
    series_data = chart.get("seriesData", [])
    playlists_series: Dict[int, int] = {}
    reach_series: Dict[int, int] = {}

    for s in series_data:
        name = s.get("name")
        data_points = s.get("data", [])
        if name == "Playlists":
            playlists_series = {point[0]: point[1] for point in data_points if len(point) == 2}
        elif name == "Playlist Reach":
            reach_series = {point[0]: point[1] for point in data_points if len(point) == 2}

    # Merge series chronologically
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

    # Calibrate current metrics against latest time-series point
    if history_records:
        latest = history_records[-1]
        if latest["playlists"] > 0:
            metrics["songstats_spotify_playlists_current"] = latest["playlists"]
        if latest["playlist_reach"] > 0:
            metrics["songstats_spotify_playlist_reach_current"] = latest["playlist_reach"]

    logger.info(
        "Extraction successful: Current Reach=%s | Current Playlists=%s | Popularity=%s | History Points=%d",
        metrics["songstats_spotify_playlist_reach_current"],
        metrics["songstats_spotify_playlists_current"],
        metrics["songstats_spotify_popularity"],
        len(history_records)
    )

    return metrics, history_records


# =====================================================================
# Rule 5: Database Persistence, WAL Mode & Delta Deduplication
# =====================================================================

def init_db(conn: sqlite3.Connection) -> None:
    """Initializes tables and WAL mode for high-concurrency container operation."""
    with conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")

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


def check_cache_ttl(conn: sqlite3.Connection, songstats_id: str, ttl_hours: int = 24) -> bool:
    """Checks if metrics for this Songstats artist are fresh (< 24 hours old)."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT updated_at, data FROM metrics_cache
        WHERE channel_id LIKE ? OR (channel_id LIKE 'songstats_spotify:%' AND data LIKE ?);
        """,
        (f"%{songstats_id}%", f'%"{songstats_id}"%')
    )
    row = cursor.fetchone()
    if not row or not row[0]:
        return False

    try:
        last_updated = datetime.fromisoformat(row[0])
        if last_updated.tzinfo is None:
            last_updated = last_updated.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        age_hours = (now - last_updated).total_seconds() / 3600.0

        if age_hours < ttl_hours:
            logger.info("Cache HIT: Metrics for artist '%s' updated %.2f hours ago (TTL: %dh). Skipping scrape.", songstats_id, age_hours, ttl_hours)
            return True
    except Exception as exc:
        logger.warning("Error evaluating cache TTL for %s: %s", songstats_id, exc)
        return False

    return False


def save_songstats_spotify_data(
    conn: sqlite3.Connection,
    spotify_id: str,
    current_data: Dict[str, Any],
    history_data: List[Dict[str, Any]]
) -> None:
    """Persists metrics snapshot with delta deduplication and stores historical records."""
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
            try:
                old_data = json.loads(row[0])
            except json.JSONDecodeError:
                old_data = {}
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

        # 2. Always UPSERT into metrics_cache (Maintains 24h TTL)
        cursor.execute(
            """
            INSERT INTO metrics_cache (channel_id, data, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET
                data = excluded.data,
                updated_at = excluded.updated_at;
            """,
            (cache_key, current_data_json, now_iso)
        )

        # 3. Conditional INSERT into historical_metrics on change (Delta Deduplication)
        if has_changed:
            cursor.execute(
                """
                INSERT INTO historical_metrics (id, platform, entity_id, data, extracted_at)
                VALUES (NULL, ?, ?, ?, ?);
                """,
                (PLATFORM_TAG, spotify_id, hist_data_json, now_iso)
            )
            logger.info("Recorded new historical entry with deltas for %s.", cache_key)
        else:
            logger.info("No metric changes detected for %s. Refreshed TTL without duplicate history.", cache_key)

        # 4. Bulk INSERT OR IGNORE into songstats_spotify_history (Daily timeline)
        if history_data:
            records = [
                (spotify_id, entry["date"], entry["playlists"], entry["playlist_reach"])
                for entry in history_data
            ]
            cursor.executemany(
                """
                INSERT OR IGNORE INTO songstats_spotify_history
                (entity_id, record_date, playlists, playlist_reach)
                VALUES (?, ?, ?, ?);
                """,
                records
            )
            logger.info("Synced %d timeline records into songstats_spotify_history.", len(records))


# =====================================================================
# Rule 2: CLI Entrypoint (Strict Arguments Required)
# =====================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Production Songstats Spotify Analytics Scraper & Ingestion Engine",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--artist",
        "-a",
        required=True,
        help="Target Songstats Artist ID or URL (e.g. 'y4ku2hlc' or 'https://songstats.com/artist/y4ku2hlc/k-law')",
    )
    parser.add_argument(
        "--slug",
        "-s",
        default=None,
        help="Optional artist URL slug (e.g. 'k-law'). Auto-derived if omitted.",
    )
    parser.add_argument(
        "--db",
        default=DB_PATH,
        help=f"Path to SQLite database (default: {DB_PATH})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force execution and bypass the 24-hour cache TTL check.",
    )
    args = parser.parse_args()

    songstats_id, slug = parse_artist_input(args.artist, args.slug)
    logger.info("Resolved artist target: Songstats ID=%s, Slug=%s", songstats_id, slug)

    # Ensure parent database folder exists
    Path(args.db).parent.mkdir(parents=True, exist_ok=True)

    try:
        conn = sqlite3.connect(args.db)
        init_db(conn)

        # Rule 5: 24-Hour Cache TTL check
        if not args.force and check_cache_ttl(conn, songstats_id):
            logger.info("Orchestrator Notice: Ingestion skipped due to fresh cache. Exiting 0.")
            conn.close()
            sys.exit(0)

        # Rule 3: Fail-Loud Network Extraction
        meta = scrape_songstats_metadata(songstats_id, slug)
        spotify_id = meta["spotify_artist_id"]

        analytics_metrics, history_records = fetch_songstats_analytics_top(songstats_id, slug)
        combined_payload = {**meta, **analytics_metrics}

        # Rule 5: Save Snapshot & Deltas
        save_songstats_spotify_data(conn, spotify_id, combined_payload, history_records)
        conn.close()

        logger.info("Songstats Spotify pipeline completed successfully for %s (%s).", songstats_id, spotify_id)

    except Exception as exc:
        logger.critical("Fatal: Database or pipeline error encountered: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()