#!/usr/bin/env python3
"""
songstats_scraper.py
--------------------
Production Songstats Scraper & Analytics Ingestion Engine for Containerized Environments.

Architectural Standards:
- Docker Volume Persistence: Default DB path set to 'data/socials_cache.db'.
- Explicit CLI Execution: Target artist ID or URL is strictly required.
- Fail-Loud Network & Validation: Terminates with sys.exit(1) on failure.
- Stream Logging: Routes structured logs to sys.stdout for Docker/Portainer log collectors.
- SQLite WAL Mode & 24h Cache TTL: Avoids duplicate scrapes and implements delta-deduplication.
"""

import argparse
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import re
import sqlite3
import sys
from typing import Any, Dict, List, Optional

# =====================================================================
# Rule 1 & Rule 4: Docker Configuration & Logging Setup
# =====================================================================

DB_PATH = "data/socials_cache.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("songstats_scraper")


# =====================================================================
# Utilities
# =====================================================================

def parse_compact_number(val: Any) -> Optional[float]:
    """
    Parses compact metric strings into raw floats for historical math and storage.
    Examples: '263K' -> 263000.0, '3.72M' -> 3720000.0, '16%' -> 16.0
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)

    s = str(val).strip().replace(",", "")
    if not s:
        return None

    try:
        if s.endswith("%"):
            return float(s[:-1])
        multiplier = 1.0
        if s.upper().endswith("K"):
            multiplier = 1_000.0
            s = s[:-1]
        elif s.upper().endswith("M"):
            multiplier = 1_000_000.0
            s = s[:-1]
        elif s.upper().endswith("B"):
            multiplier = 1_000_000_000.0
            s = s[:-1]
        return float(s) * multiplier
    except ValueError:
        return None


def extract_artist_id_from_url(url_or_id: str) -> str:
    """Extracts alphanumeric Songstats ID from URL or bare string."""
    cleaned = url_or_id.strip()
    match = re.search(r"songstats\.com/artist/([a-zA-Z0-9]+)", cleaned)
    if match:
        return match.group(1)
    if "/" in cleaned or "?" in cleaned:
        tokens = [t for t in cleaned.strip("/").split("/") if t]
        if tokens:
            return tokens[-1]
    return cleaned


# =====================================================================
# Rule 5: Database Schema, WAL Mode, Cache TTL & Delta Deduplication
# =====================================================================

def init_database(db_path: str = DB_PATH) -> None:
    """
    Initializes SQLite with WAL mode enabled and creates all master analytics tables.
    Performs dynamic column migrations for newly linked streaming platforms.
    """
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Enforce WAL mode and foreign key integrity
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=NORMAL;")
        cursor.execute("PRAGMA foreign_keys=ON;")

        # 1. 24-Hour TTL Metrics Cache Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS metrics_cache (
            platform TEXT,
            entity_id TEXT,
            metric_type TEXT,
            value REAL,
            updated_at TIMESTAMP,
            PRIMARY KEY (platform, entity_id, metric_type)
        );
        """)

        # 2. Historical Metrics (Delta Deduplication)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS historical_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            platform TEXT,
            entity_id TEXT,
            metric_type TEXT,
            value REAL,
            recorded_at TIMESTAMP
        );
        """)
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_historical_metrics_lookup
        ON historical_metrics (platform, entity_id, metric_type, recorded_at);
        """)

        # 3. Master Identities (The Rosetta Stone)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS artist_identities (
            songstats_id TEXT PRIMARY KEY,
            artist_name TEXT,
            spotify_id TEXT,
            youtube_id TEXT,
            instagram_handle TEXT,
            soundcloud_handle TEXT,
            apple_music_id TEXT,
            beatport_id TEXT,
            deezer_id TEXT,
            amazon_id TEXT,
            tidal_id TEXT,
            shazam_id TEXT,
            updated_at TIMESTAMP
        );
        """)

        # Auto-migration for newly added platform columns
        cursor.execute("PRAGMA table_info(artist_identities);")
        existing_cols = {row[1] for row in cursor.fetchall()}
        columns_to_ensure = [
            ("apple_music_id", "TEXT"),
            ("beatport_id", "TEXT"),
            ("deezer_id", "TEXT"),
            ("amazon_id", "TEXT"),
            ("tidal_id", "TEXT"),
            ("shazam_id", "TEXT"),
        ]
        for col_name, col_type in columns_to_ensure:
            if col_name not in existing_cols:
                logger.info(f"Applying schema migration: Adding column '{col_name}' to 'artist_identities'.")
                cursor.execute(f"ALTER TABLE artist_identities ADD COLUMN {col_name} {col_type};")

        # 4. Artist Overview & Musical DNA
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS artist_overview (
            songstats_id TEXT PRIMARY KEY,
            artist_name TEXT,
            bio TEXT,
            image_url TEXT,
            large_image_url TEXT,
            genres TEXT,
            avg_tempo REAL,
            avg_key TEXT,
            avg_duration TEXT,
            time_signature TEXT,
            danceability INTEGER,
            energy INTEGER,
            valence INTEGER,
            acousticness INTEGER,
            loudness_db TEXT,
            updated_at TIMESTAMP,
            FOREIGN KEY (songstats_id) REFERENCES artist_identities (songstats_id)
        );
        """)

        # 5. Overall Performance Stats
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS artist_performance (
            songstats_id TEXT,
            stat_type TEXT,
            stat_name TEXT,
            display_value TEXT,
            numeric_value REAL,
            source_ids TEXT,
            updated_at TIMESTAMP,
            PRIMARY KEY (songstats_id, stat_type),
            FOREIGN KEY (songstats_id) REFERENCES artist_identities (songstats_id)
        );
        """)

        # 6. Follower Breakdown by Platform
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS artist_follower_breakdown (
            songstats_id TEXT,
            platform TEXT,
            followers INTEGER,
            updated_at TIMESTAMP,
            PRIMARY KEY (songstats_id, platform),
            FOREIGN KEY (songstats_id) REFERENCES artist_identities (songstats_id)
        );
        """)

        # 7. Collaborators
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS artist_collaborators (
            songstats_id TEXT,
            collaborator_id TEXT,
            name TEXT,
            profile_url TEXT,
            image_url TEXT,
            updated_at TIMESTAMP,
            PRIMARY KEY (songstats_id, collaborator_id),
            FOREIGN KEY (songstats_id) REFERENCES artist_identities (songstats_id)
        );
        """)

        # 8. Related Artists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS artist_related (
            songstats_id TEXT,
            related_artist_id TEXT,
            name TEXT,
            monthly_listeners INTEGER,
            sources TEXT,
            image_url TEXT,
            updated_at TIMESTAMP,
            PRIMARY KEY (songstats_id, related_artist_id),
            FOREIGN KEY (songstats_id) REFERENCES artist_identities (songstats_id)
        );
        """)

        # 9. Top Performing Tracks
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS artist_top_tracks (
            songstats_id TEXT,
            track_id TEXT,
            track_name TEXT,
            artist_name TEXT,
            popularity_pct REAL,
            rank_order INTEGER,
            preview_url TEXT,
            image_url TEXT,
            updated_at TIMESTAMP,
            PRIMARY KEY (songstats_id, track_id),
            FOREIGN KEY (songstats_id) REFERENCES artist_identities (songstats_id)
        );
        """)

        # 10. Recent Releases
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS artist_recent_releases (
            songstats_id TEXT,
            track_id TEXT,
            track_name TEXT,
            release_date TEXT,
            image_url TEXT,
            updated_at TIMESTAMP,
            PRIMARY KEY (songstats_id, track_id),
            FOREIGN KEY (songstats_id) REFERENCES artist_identities (songstats_id)
        );
        """)

        # 11. Recent Highlights (Activity Feed & Syncs)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS artist_highlights (
            highlight_id INTEGER PRIMARY KEY,
            songstats_id TEXT,
            track_name TEXT,
            track_id TEXT,
            event_date TEXT,
            source TEXT,
            activity_type TEXT,
            notification_text TEXT,
            comment_text TEXT,
            external_url TEXT,
            rank_score REAL,
            updated_at TIMESTAMP,
            FOREIGN KEY (songstats_id) REFERENCES artist_identities (songstats_id)
        );
        """)

        conn.commit()


def check_cache_ttl(artist_id: str, db_path: str = DB_PATH, ttl_hours: int = 24) -> bool:
    """
    Rule 5: Evaluates if metrics for this artist were refreshed within the 24-hour TTL window.
    """
    if not Path(db_path).exists():
        return False

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT MAX(updated_at) FROM metrics_cache
            WHERE platform = 'songstats' AND entity_id = ?;
            """,
            (artist_id,),
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
                logger.info(
                    f"Cache HIT: Artist '{artist_id}' metrics are fresh (age: {age_hours:.2f}h, TTL: {ttl_hours}h). "
                    "Skipping browser execution."
                )
                return True
        except Exception as e:
            logger.warning(f"Error checking cache timestamp '{row[0]}': {e}")
            return False

    return False


def record_metrics_and_deltas(
    cursor: sqlite3.Cursor,
    platform: str,
    entity_id: str,
    metric_type: str,
    value: Optional[float],
    now_iso: str,
) -> None:
    """
    Rule 5: Upserts metrics_cache and applies delta-deduplication to historical_metrics.
    Only inserts a new row in historical_metrics if the value has changed.
    """
    if value is None:
        return

    # Check last recorded value for delta-deduplication
    cursor.execute(
        """
        SELECT value FROM historical_metrics
        WHERE platform = ? AND entity_id = ? AND metric_type = ?
        ORDER BY recorded_at DESC, id DESC
        LIMIT 1;
        """,
        (platform, entity_id, metric_type),
    )
    row = cursor.fetchone()
    last_value = row[0] if row else None

    # Insert historical metric ONLY if value changed (or no prior record exists)
    if last_value is None or abs(last_value - value) > 1e-6:
        cursor.execute(
            """
            INSERT INTO historical_metrics (
                platform, entity_id, metric_type, value, recorded_at
            ) VALUES (?, ?, ?, ?, ?);
            """,
            (platform, entity_id, metric_type, value, now_iso),
        )
        logger.debug(f"Delta recorded for {platform}:{entity_id}:{metric_type} -> {value} (prev: {last_value})")
    else:
        logger.debug(f"Delta deduplicated for {platform}:{entity_id}:{metric_type} -> {value} (unchanged)")

    # Update active 24h cache
    cursor.execute(
        """
        INSERT INTO metrics_cache (
            platform, entity_id, metric_type, value, updated_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(platform, entity_id, metric_type) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at;
        """,
        (platform, entity_id, metric_type, value, now_iso),
    )


# =====================================================================
# Rule 3: Fail-Loud Browser Data Fetcher
# =====================================================================

def fetch_dashboard_via_browser(artist_id: str, headless: bool = True) -> Dict[str, Any]:
    """
    Rule 3: Navigates to Songstats using Playwright to bypass Cloudflare Turnstile/JSD.
    Captures live payloads directly in memory. Fails loudly on timeouts or non-200s.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        logger.critical("Playwright runtime missing. Ensure playwright is installed in container.", exc_info=True)
        sys.exit(1)

    target_url = f"https://songstats.com/artist/{artist_id}"
    results: Dict[str, Any] = {}
    http_errors: List[str] = []

    def on_response(response):
        url = response.url
        if "data.songstats.com" in url:
            if response.status >= 400:
                http_errors.append(f"HTTP {response.status} on endpoint: {url}")
                return

            if response.status == 200:
                try:
                    ct = response.headers.get("content-type", "")
                    if "application/json" in ct:
                        body = response.json()
                        if "account" in body and "account" not in results:
                            results["account"] = body
                            logger.info("Captured [account] payload.")
                        elif "overviewInfo" in body and "stats" in body.get("overviewInfo", {}) and "top" not in results:
                            results["top"] = body
                            logger.info("Captured [top] payload.")
                        elif ("tabData" in body or "trackData" in body) and "bottom" not in results:
                            results["bottom"] = body
                            logger.info("Captured [bottom] payload.")
                except Exception as ex:
                    logger.warning(f"Error parsing JSON response from {url}: {ex}")

    logger.info(f"Initiating headless browser navigation to: {target_url}")
    with sync_playwright() as p:
        try:
            # Optimized Chromium launch flags for containerized Docker/Portainer environments
            browser = p.chromium.launch(
                headless=headless,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1440, "height": 900},
            )
            page = context.new_page()
            page.on("response", on_response)

            response = page.goto(target_url, wait_until="domcontentloaded", timeout=45000)
            if response and response.status >= 400:
                raise RuntimeError(f"Main page returned non-200 status code: {response.status}")

            # Await all three asynchronous SPA endpoints
            for _ in range(12):
                if all(k in results for k in ["account", "top", "bottom"]):
                    break
                page.wait_for_timeout(1000)

            browser.close()
        except Exception as e:
            logger.critical(f"Fatal browser navigation error: {e}", exc_info=True)
            sys.exit(1)

    if http_errors:
        for err in http_errors:
            logger.error(err)
        logger.critical("Fatal: Songstats API returned non-200 HTTP responses.")
        sys.exit(1)

    required_keys = ["account", "top", "bottom"]
    missing = [k for k in required_keys if k not in results]
    if missing:
        logger.critical(f"Fatal: Extraction failed to capture required API payloads: {missing}")
        sys.exit(1)

    for key in required_keys:
        if results[key].get("result") != "success":
            logger.critical(f"Fatal: Payload '{key}' indicated unfulfilled query: {results[key].get('message')}")
            sys.exit(1)

    return results


# =====================================================================
# Ingestion Pipeline
# =====================================================================

def parse_identities_from_links(account_data: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """Extracts platform IDs and usernames from account metadata."""
    links = account_data.get("allLinks") or account_data.get("links") or []

    res = {
        "spotify_id": None,
        "youtube_id": None,
        "instagram_handle": None,
        "soundcloud_handle": None,
        "apple_music_id": None,
        "beatport_id": None,
        "deezer_id": None,
        "amazon_id": None,
        "tidal_id": None,
        "shazam_id": None,
    }

    for item in links:
        source = item.get("source")
        url = item.get("link", "")
        username = item.get("username", "")

        if source == "spotify" and not res["spotify_id"]:
            m = re.search(r"artist/([a-zA-Z0-9]+)", url)
            if m:
                res["spotify_id"] = m.group(1)

        elif source == "youtube" and not res["youtube_id"]:
            m = re.search(r"channel/([a-zA-Z0-9_-]+)", url)
            if m:
                res["youtube_id"] = m.group(1)

        elif source == "instagram" and not res["instagram_handle"]:
            clean_user = username.lstrip("@").strip()
            if clean_user:
                res["instagram_handle"] = clean_user
            else:
                m = re.search(r"instagram\.com/([a-zA-Z0-9_.]+)", url)
                if m:
                    res["instagram_handle"] = m.group(1).rstrip("/")

        elif source == "soundcloud" and not res["soundcloud_handle"]:
            clean_user = username.lstrip("@").strip()
            if clean_user:
                res["soundcloud_handle"] = clean_user
            else:
                m = re.search(r"soundcloud\.com/([a-zA-Z0-9_-]+)", url)
                if m:
                    res["soundcloud_handle"] = m.group(1).rstrip("/")

        elif source == "apple_music" and not res["apple_music_id"]:
            m = re.search(r"artist/(?:.*?/)?(\d+)", url)
            if m:
                res["apple_music_id"] = m.group(1)

        elif source == "beatport" and not res["beatport_id"]:
            m = re.search(r"artist/(?:.*?/)?(\d+)", url)
            if m:
                res["beatport_id"] = m.group(1)

        elif source == "deezer" and not res["deezer_id"]:
            m = re.search(r"artist/(\d+)", url)
            if m:
                res["deezer_id"] = m.group(1)

        elif source == "amazon" and not res["amazon_id"]:
            m = re.search(r"artists/([a-zA-Z0-9]+)", url)
            if m:
                res["amazon_id"] = m.group(1)

        elif source == "tidal" and not res["tidal_id"]:
            m = re.search(r"artist/(\d+)", url)
            if m:
                res["tidal_id"] = m.group(1)

        elif source == "shazam" and not res["shazam_id"]:
            m = re.search(r"artist/(?:.*?/)?(\d+)", url)
            if m:
                res["shazam_id"] = m.group(1)

    return res


def save_full_dashboard_to_sqlite(
    account_payload: Dict[str, Any],
    top_payload: Dict[str, Any],
    bottom_payload: Dict[str, Any],
    db_path: str = DB_PATH,
) -> None:
    """Performs atomic relational UPSERTs and updates delta metrics."""
    init_database(db_path)
    now_iso = datetime.now(timezone.utc).isoformat()

    account = account_payload.get("account", {})
    songstats_id = account.get("idUnique")
    artist_name = account.get("name", "")

    if not songstats_id or not artist_name:
        logger.critical("Fatal: Extracted payload missing essential 'idUnique' or 'name' values.")
        sys.exit(1)

    logger.info(f"Persisting analytics warehouse for '{artist_name}' ({songstats_id}) into '{db_path}'...")
    identities = parse_identities_from_links(account)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # 1. Master Identities
        cursor.execute(
            """
            INSERT INTO artist_identities (
                songstats_id, artist_name, spotify_id, youtube_id,
                instagram_handle, soundcloud_handle, apple_music_id,
                beatport_id, deezer_id, amazon_id, tidal_id, shazam_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(songstats_id) DO UPDATE SET
                artist_name = excluded.artist_name,
                spotify_id = excluded.spotify_id,
                youtube_id = excluded.youtube_id,
                instagram_handle = excluded.instagram_handle,
                soundcloud_handle = excluded.soundcloud_handle,
                apple_music_id = excluded.apple_music_id,
                beatport_id = excluded.beatport_id,
                deezer_id = excluded.deezer_id,
                amazon_id = excluded.amazon_id,
                tidal_id = excluded.tidal_id,
                shazam_id = excluded.shazam_id,
                updated_at = excluded.updated_at;
            """,
            (
                songstats_id,
                artist_name,
                identities["spotify_id"],
                identities["youtube_id"],
                identities["instagram_handle"],
                identities["soundcloud_handle"],
                identities["apple_music_id"],
                identities["beatport_id"],
                identities["deezer_id"],
                identities["amazon_id"],
                identities["tidal_id"],
                identities["shazam_id"],
                now_iso,
            ),
        )

        # 2. Overview & Audio DNA
        overview_info = top_payload.get("overviewInfo", {})
        genres = json.dumps(overview_info.get("genres", []))
        audio_features = overview_info.get("audioFeatureData", {})
        summary_items = {item["key"]: item["value"] for item in audio_features.get("summaryItems", [])}

        polar_cats = audio_features.get("polarData", {}).get("categories", [])
        polar_vals = audio_features.get("polarData", {}).get("data", [])
        polar_dict = dict(zip(polar_cats, polar_vals)) if polar_cats and polar_vals else {}
        polar_tooltips = dict(zip(polar_cats, audio_features.get("polarData", {}).get("tooltipValues", [])))

        cursor.execute(
            """
            INSERT INTO artist_overview (
                songstats_id, artist_name, bio, image_url, large_image_url,
                genres, avg_tempo, avg_key, avg_duration, time_signature,
                danceability, energy, valence, acousticness, loudness_db, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(songstats_id) DO UPDATE SET
                artist_name = excluded.artist_name,
                bio = excluded.bio,
                image_url = excluded.image_url,
                large_image_url = excluded.large_image_url,
                genres = excluded.genres,
                avg_tempo = excluded.avg_tempo,
                avg_key = excluded.avg_key,
                avg_duration = excluded.avg_duration,
                time_signature = excluded.time_signature,
                danceability = excluded.danceability,
                energy = excluded.energy,
                valence = excluded.valence,
                acousticness = excluded.acousticness,
                loudness_db = excluded.loudness_db,
                updated_at = excluded.updated_at;
            """,
            (
                songstats_id,
                artist_name,
                account.get("bio", ""),
                account.get("imageUrl"),
                account.get("largeImageUrl"),
                genres,
                float(summary_items["tempo"]) if "tempo" in summary_items else None,
                summary_items.get("key"),
                summary_items.get("duration"),
                summary_items.get("time_signature"),
                polar_dict.get("Danceability"),
                polar_dict.get("Energy"),
                polar_dict.get("Valence"),
                polar_dict.get("Acousticness"),
                polar_tooltips.get("Loudness"),
                now_iso,
            ),
        )

        # 3. Overall Performance Stats & Delta Deduplication
        stats_list = overview_info.get("stats", [])
        for stat in stats_list:
            stat_type = stat.get("statType")
            name = stat.get("name")
            val_str = stat.get("value", "")
            numeric_val = parse_compact_number(val_str)
            sources = ",".join(stat.get("sourceIds", []))

            cursor.execute(
                """
                INSERT INTO artist_performance (
                    songstats_id, stat_type, stat_name, display_value, numeric_value, source_ids, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(songstats_id, stat_type) DO UPDATE SET
                    stat_name = excluded.stat_name,
                    display_value = excluded.display_value,
                    numeric_value = excluded.numeric_value,
                    source_ids = excluded.source_ids,
                    updated_at = excluded.updated_at;
                """,
                (songstats_id, stat_type, name, val_str, numeric_val, sources, now_iso),
            )

            # Rule 5: Cache & Delta Recording
            record_metrics_and_deltas(cursor, "songstats", songstats_id, stat_type, numeric_val, now_iso)

        # 4. Follower Breakdown by Platform & Delta Recording
        follower_items = overview_info.get("followerBreakdown", {}).get("data", [])
        for fb in follower_items:
            platform = fb.get("sourceId") or fb.get("name", "").lower()
            count = fb.get("y", 0)
            cursor.execute(
                """
                INSERT INTO artist_follower_breakdown (
                    songstats_id, platform, followers, updated_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(songstats_id, platform) DO UPDATE SET
                    followers = excluded.followers,
                    updated_at = excluded.updated_at;
                """,
                (songstats_id, platform, count, now_iso),
            )
            # Rule 5: Cache & Delta Recording
            record_metrics_and_deltas(cursor, platform, songstats_id, "followers", float(count), now_iso)

        # 5. Collaborators
        entity_links = overview_info.get("entityLinks", [])
        for el in entity_links:
            if "collaborator" in el.get("name", "").lower():
                for c in el.get("data", []):
                    c_id = c.get("idUnique")
                    c_name = c.get("text")
                    c_to = c.get("to")
                    c_img = c.get("imageUrl")
                    if c_id and c_name:
                        cursor.execute(
                            """
                            INSERT INTO artist_collaborators (
                                songstats_id, collaborator_id, name, profile_url, image_url, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT(songstats_id, collaborator_id) DO UPDATE SET
                                name = excluded.name,
                                profile_url = excluded.profile_url,
                                image_url = excluded.image_url,
                                updated_at = excluded.updated_at;
                            """,
                            (songstats_id, c_id, c_name, c_to, c_img, now_iso),
                        )

        # 6. Related Artists
        tab_data = bottom_payload.get("tabData") or bottom_payload.get("overviewInfo") or {}
        related_network = tab_data.get("relatedNetwork", {}).get("items", [])
        for rel in related_network:
            rel_id = rel.get("idUnique")
            rel_name = rel.get("text")
            listeners = rel.get("monthlyListeners")
            rel_img = rel.get("imageUrl")
            rel_sources = ",".join(rel.get("sourceIds", []))
            if rel_id and rel_name:
                cursor.execute(
                    """
                    INSERT INTO artist_related (
                        songstats_id, related_artist_id, name, monthly_listeners, sources, image_url, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(songstats_id, related_artist_id) DO UPDATE SET
                        name = excluded.name,
                        monthly_listeners = excluded.monthly_listeners,
                        sources = excluded.sources,
                        image_url = excluded.image_url,
                        updated_at = excluded.updated_at;
                    """,
                    (songstats_id, rel_id, rel_name, listeners, rel_sources, rel_img, now_iso),
                )

        # 7. Top Performing Tracks
        track_data = bottom_payload.get("trackData") or {}
        list_data = track_data.get("listData") or tab_data.get("listData") or []
        for section in list_data:
            if "top performing" in section.get("headerText", "").lower():
                items_matrix = section.get("items", [])
                rank_counter = 1
                for sublist in items_matrix:
                    for t in sublist:
                        t_id = t.get("idUnique")
                        t_name = t.get("trackName")
                        t_artist = t.get("artistName")
                        t_pop = parse_compact_number(t.get("primaryValue"))
                        t_img = t.get("imageUrl")
                        t_preview = t.get("previewUrl")

                        if t_id and t_name:
                            cursor.execute(
                                """
                                INSERT INTO artist_top_tracks (
                                    songstats_id, track_id, track_name, artist_name,
                                    popularity_pct, rank_order, preview_url, image_url, updated_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT(songstats_id, track_id) DO UPDATE SET
                                    track_name = excluded.track_name,
                                    artist_name = excluded.artist_name,
                                    popularity_pct = excluded.popularity_pct,
                                    rank_order = excluded.rank_order,
                                    preview_url = excluded.preview_url,
                                    image_url = excluded.image_url,
                                    updated_at = excluded.updated_at;
                                """,
                                (songstats_id, t_id, t_name, t_artist, t_pop, rank_counter, t_preview, t_img, now_iso),
                            )
                            rank_counter += 1

        # 8. Recent Releases
        rel_tracks_sec = track_data.get("relatedTracks") or tab_data.get("relatedTracks") or []
        for r_sec in rel_tracks_sec:
            for rt in r_sec.get("relatedTracks", []):
                rt_id = rt.get("idUnique")
                rt_name = rt.get("trackName")
                rt_date = rt.get("releaseDate")
                rt_img = rt.get("imageUrl")
                if rt_id and rt_name:
                    cursor.execute(
                        """
                        INSERT INTO artist_recent_releases (
                            songstats_id, track_id, track_name, release_date, image_url, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(songstats_id, track_id) DO UPDATE SET
                            track_name = excluded.track_name,
                            release_date = excluded.release_date,
                            image_url = excluded.image_url,
                            updated_at = excluded.updated_at;
                        """,
                        (songstats_id, rt_id, rt_name, rt_date, rt_img, now_iso),
                    )

        # 9. Recent Highlights (Activity & Sync Feed)
        highlights = tab_data.get("highlightItems") or bottom_payload.get("highlightItems") or []
        for h in highlights:
            h_id = h.get("id")
            if not h_id:
                continue
            cursor.execute(
                """
                INSERT INTO artist_highlights (
                    highlight_id, songstats_id, track_name, track_id, event_date,
                    source, activity_type, notification_text, comment_text,
                    external_url, rank_score, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(highlight_id) DO UPDATE SET
                    track_name = excluded.track_name,
                    track_id = excluded.track_id,
                    event_date = excluded.event_date,
                    source = excluded.source,
                    activity_type = excluded.activity_type,
                    notification_text = excluded.notification_text,
                    comment_text = excluded.comment_text,
                    external_url = excluded.external_url,
                    rank_score = excluded.rank_score,
                    updated_at = excluded.updated_at;
                """,
                (
                    h_id,
                    songstats_id,
                    h.get("trackName"),
                    h.get("idUnique"),
                    h.get("date"),
                    h.get("source"),
                    h.get("activityType"),
                    h.get("notificationText"),
                    h.get("commentText"),
                    h.get("externalUrl"),
                    float(h.get("rank", 0.0)) if h.get("rank") is not None else None,
                    now_iso,
                ),
            )

        conn.commit()

    logger.info(f"Successfully finalized ingestion for '{artist_name}' ({songstats_id}).")


# =====================================================================
# Database Logging Summary
# =====================================================================

def log_database_summary(artist_id: str, db_path: str = DB_PATH) -> None:
    """Queries and emits an aggregated intelligence summary to stdout."""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        c.execute("SELECT * FROM artist_identities WHERE songstats_id = ?", (artist_id,))
        ident = c.fetchone()
        if not ident:
            logger.warning(f"No identity record located in database for ID: {artist_id}")
            return

        logger.info(f"=== DATABASE AUDIT: {ident['artist_name']} ({artist_id}) ===")
        logger.info(
            f"Platforms Resolved: Spotify={ident['spotify_id']} | YouTube={ident['youtube_id']} | "
            f"IG=@{ident['instagram_handle']} | AppleMusic={ident['apple_music_id']}"
        )

        c.execute("SELECT stat_name, display_value FROM artist_performance WHERE songstats_id = ?", (artist_id,))
        stats = c.fetchall()
        stats_str = ", ".join([f"{s['stat_name']}: {s['display_value']}" for s in stats])
        logger.info(f"Performance Stats: {stats_str}")

        c.execute("SELECT platform, followers FROM artist_follower_breakdown WHERE songstats_id = ?", (artist_id,))
        followers = c.fetchall()
        followers_str = ", ".join([f"{f['platform']}: {f['followers']:,}" for f in followers])
        logger.info(f"Follower Distribution: {followers_str}")

        c.execute("SELECT COUNT(*) FROM artist_top_tracks WHERE songstats_id = ?", (artist_id,))
        top_tracks_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM artist_highlights WHERE songstats_id = ?", (artist_id,))
        highlights_count = c.fetchone()[0]
        logger.info(f"Associated Entities: Top Tracks={top_tracks_count} | Recent Highlights={highlights_count}")


# =====================================================================
# Rule 2: CLI Entrypoint (Strict Arguments Required)
# =====================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Production Songstats Scraper & Analytics Ingestion Engine",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # Strictly required argument - exits with status 2 if missing
    parser.add_argument(
        "--artist",
        "-a",
        required=True,
        help="Target Songstats Artist ID or URL (e.g. 'y4ku2hlc' or 'https://songstats.com/artist/y4ku2hlc/k-law')",
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
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run browser in visible mode (default: headless).",
    )
    args = parser.parse_args()

    artist_id = extract_artist_id_from_url(args.artist)
    logger.info(f"Target Songstats Artist ID resolved to: {artist_id}")

    # FIX: Initialize the database tables before checking the cache!
    init_database(args.db)

    # Rule 5: 24-Hour Cache TTL Evaluation
    if not args.force and check_cache_ttl(artist_id=artist_id, db_path=args.db):
        logger.info("Orchestrator Notice: Ingestion skipped due to fresh cache. Exiting 0.")
        sys.exit(0)

    # Rule 3: Fail-Loud Network Extraction
    data = fetch_dashboard_via_browser(artist_id=artist_id, headless=not args.headed)

    # Relational & Delta Persistence
    save_full_dashboard_to_sqlite(
        account_payload=data["account"],
        top_payload=data["top"],
        bottom_payload=data["bottom"],
        db_path=args.db,
    )

    # Rule 4: Standardized Logging Output
    log_database_summary(artist_id=artist_id, db_path=args.db)
    logger.info(f"Completed ingestion cycle successfully for artist: {artist_id}")


if __name__ == "__main__":
    main()