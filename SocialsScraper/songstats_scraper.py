#!/usr/bin/env python3
"""
songstats_scraper.py
--------------------
Local Multi-Platform Music Analytics Pipeline - Songstats Ingestion Engine.

Ingests full artist intelligence from Songstats:
- Master Identities (Rosetta Stone: Spotify, YouTube, IG, Apple Music, SoundCloud, etc.)
- Overview & Musical Audio DNA (Tempo, Key, Danceability, Energy, Valence, Genres)
- Global Performance & Platform Follower Breakdown
- Collaborator Network
- Related Artists & Monthly Listeners
- Top Performing Tracks (Rankings, Popularity %, MP3 previews)
- Recent Releases & Activity Highlights (Radio spins, Editorial playlists, Syncs)

Includes automatic SQLite schema migration and local JSON caching.
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
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("songstats_scraper")

DEFAULT_ARTIST_ID = "y4ku2hlc"
DEFAULT_DB_PATH = "socials_cache.db"
DEFAULT_DUMP_DIR = "songstats_api_dumps"


# =====================================================================
# Utilities & Parsers
# =====================================================================

def parse_compact_number(val: Any) -> Optional[float]:
    """
    Parses compact metric strings into raw floats for arithmetic querying.
    Examples:
        '263K'  -> 263000.0
        '3.72M' -> 3720000.0
        '16%'   -> 16.0
        '3321'  -> 3321.0
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


def extract_artist_id_from_url(url: str) -> str:
    """Extracts alphanumeric Songstats ID from URL or bare string."""
    match = re.search(r"songstats\.com/artist/([a-zA-Z0-9]+)", url)
    return match.group(1) if match else url.strip()


# =====================================================================
# Database Schema & Auto-Migration
# =====================================================================

def init_database(db_path: str = DEFAULT_DB_PATH) -> None:
    """Creates tables and automatically migrates missing columns on existing tables."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # 1. Master Identity Resolution Table
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

        # Auto-migration: Check if existing table has all columns, add any that are missing
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
                logger.info(f"Migrating schema: adding column '{col_name}' to 'artist_identities'")
                cursor.execute(f"ALTER TABLE artist_identities ADD COLUMN {col_name} {col_type};")

        # 2. Artist Overview & Audio DNA
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

        # 3. Overall Performance Metrics
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

        # 4. Platform Follower Breakdown
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

        # 5. Collaborators
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

        # 6. Related Artists
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

        # 7. Top Performing Tracks
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

        # 8. Recent Releases
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

        # 9. Recent Highlights (Activity & Sync Feed)
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


# =====================================================================
# Ingestion & Normalization
# =====================================================================

def parse_identities_from_links(account_data: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """Extracts platform IDs and usernames from account info links."""
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
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    """Performs atomic relational UPSERTs across all analytics tables."""
    init_database(db_path)
    now_iso = datetime.now(timezone.utc).isoformat()

    account = account_payload.get("account", {})
    songstats_id = account.get("idUnique")
    artist_name = account.get("name", "")

    if not songstats_id:
        raise ValueError("Invalid payload: Missing account.idUnique")

    logger.info(f"Persisting full intelligence for '{artist_name}' ({songstats_id}) into '{db_path}'...")
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

        # 2. Overview & Musical Audio DNA
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

        # 3. Overall Performance Stats
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

        # 4. Follower Breakdown by Platform
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

        # 9. Recent Highlights (Activity & Feed Sync)
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


# =====================================================================
# Live Fetch Engine (Playwright Headless) & Local Dump Loader
# =====================================================================

def fetch_dashboard_via_browser(artist_id: str, headless: bool = True) -> Dict[str, Any]:
    """
    Launches headless Chromium via Playwright, executes Cloudflare jsd/main.js,
    and intercepts the 3 API payloads from data.songstats.com directly in memory.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise RuntimeError("Playwright is not installed in this environment. Run 'pip install playwright'.")

    target_url = f"https://songstats.com/artist/{artist_id}"
    results: Dict[str, Any] = {}

    def on_response(response):
        url = response.url
        if "data.songstats.com" in url and response.status == 200:
            try:
                ct = response.headers.get("content-type", "")
                if "application/json" in ct:
                    body = response.json()
                    if "account" in body and "account" not in results:
                        results["account"] = body
                        logger.info("Captured [account] API payload.")
                    elif "overviewInfo" in body and "stats" in body.get("overviewInfo", {}) and "top" not in results:
                        results["top"] = body
                        logger.info("Captured [top] API payload.")
                    elif ("tabData" in body or "trackData" in body) and "bottom" not in results:
                        results["bottom"] = body
                        logger.info("Captured [bottom] API payload.")
            except Exception:
                pass

    logger.info(f"Navigating to {target_url} via Playwright to solve Cloudflare challenge...")
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled"],
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

        try:
            page.goto(target_url, wait_until="domcontentloaded", timeout=40000)
        except Exception as e:
            logger.debug(f"Navigation warning: {e}")

        # Wait up to 10 seconds for all three dynamic payloads to be intercepted
        for _ in range(10):
            if all(k in results for k in ["account", "top", "bottom"]):
                break
            page.wait_for_timeout(1000)

        browser.close()

    if not all(k in results for k in ["account", "top", "bottom"]):
        missing = [k for k in ["account", "top", "bottom"] if k not in results]
        raise RuntimeError(f"Browser navigation completed but missed payloads: {missing}")

    # Auto-cache to DEFAULT_DUMP_DIR for future runs
    try:
        dump_dir = Path(DEFAULT_DUMP_DIR)
        dump_dir.mkdir(parents=True, exist_ok=True)
        with open(dump_dir / f"{artist_id}_account.json", "w", encoding="utf-8") as f:
            json.dump(results["account"], f, indent=2)
        with open(dump_dir / f"{artist_id}_top.json", "w", encoding="utf-8") as f:
            json.dump(results["top"], f, indent=2)
        with open(dump_dir / f"{artist_id}_bottom.json", "w", encoding="utf-8") as f:
            json.dump(results["bottom"], f, indent=2)
        logger.info(f"Payloads cached to '{dump_dir}/'")
    except Exception as e:
        logger.debug(f"Cache write error: {e}")

    return results


def locate_cached_payloads(search_dirs: List[Path]) -> Optional[Dict[str, Any]]:
    """
    Intelligently inspects JSON files in candidate directories to match
    Songstats payloads by structure, regardless of exact filenames.
    """
    results = {}
    for directory in search_dirs:
        if not directory.exists() or not directory.is_dir():
            continue

        for json_file in sorted(directory.glob("*.json")):
            if json_file.name.startswith("_"):
                continue

            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                if not isinstance(data, dict):
                    continue

                if "account" in data and "account" not in results:
                    results["account"] = data
                    logger.info(f"Loaded 'account' payload from: {json_file.name}")
                elif "overviewInfo" in data and "stats" in data.get("overviewInfo", {}) and "top" not in results:
                    results["top"] = data
                    logger.info(f"Loaded 'analytics_top' payload from: {json_file.name}")
                elif ("tabData" in data or "trackData" in data) and "bottom" not in results:
                    results["bottom"] = data
                    logger.info(f"Loaded 'analytics_bottom' payload from: {json_file.name}")

            except Exception:
                continue

        if all(k in results for k in ["account", "top", "bottom"]):
            return results

    return results if all(k in results for k in ["account", "top", "bottom"]) else None


# =====================================================================
# Database Verification Summary
# =====================================================================

def print_database_summary(artist_id: str, db_path: str = DEFAULT_DB_PATH) -> None:
    """Queries and displays an aggregated intelligence summary from SQLite."""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        print("\n" + "=" * 75)
        print(f"       SONGSTATS DASHBOARD WAREHOUSE: {artist_id.upper()}")
        print("=" * 75)

        # 1. Master Identities
        c.execute("SELECT * FROM artist_identities WHERE songstats_id = ?", (artist_id,))
        ident = c.fetchone()
        if ident:
            print("\n[+] MASTER IDENTITIES (Rosetta Stone):")
            print(f"  Artist Name       : {ident['artist_name']}")
            print(f"  Spotify ID        : {ident['spotify_id']}")
            print(f"  YouTube ID        : {ident['youtube_id']}")
            print(f"  Instagram Handle  : @{ident['instagram_handle']}")
            print(f"  SoundCloud Handle : {ident['soundcloud_handle']}")
            print(f"  Apple Music ID    : {ident['apple_music_id']}")
            print(f"  Beatport ID       : {ident['beatport_id']}")
            print(f"  Deezer ID         : {ident['deezer_id']}")

        # 2. Overview & Audio DNA
        c.execute("SELECT * FROM artist_overview WHERE songstats_id = ?", (artist_id,))
        overview = c.fetchone()
        if overview:
            print("\n[+] OVERVIEW & AUDIO DNA:")
            genres_list = json.loads(overview["genres"]) if overview["genres"] else []
            print(f"  Genres            : {', '.join(genres_list)}")
            print(f"  Catalog Tempo     : {overview['avg_tempo']} BPM (Key of {overview['avg_key']})")
            print(f"  Avg Track Duration: {overview['avg_duration']} ({overview['time_signature']})")
            print(f"  Danceability / Eng: {overview['danceability']}% / {overview['energy']}%")
            print(f"  Valence / Acoustic: {overview['valence']}% / {overview['acousticness']}%")
            print(f"  Avg Loudness      : {overview['loudness_db']}")

        # 3. Performance Summary
        c.execute("SELECT stat_name, display_value, source_ids FROM artist_performance WHERE songstats_id = ?", (artist_id,))
        stats = c.fetchall()
        print("\n[+] PERFORMANCE STATS:")
        for s in stats:
            print(f"  - {s['stat_name']:<18}: {s['display_value']:<10} (sources: {s['source_ids']})")

        # 4. Follower Breakdown
        c.execute("SELECT platform, followers FROM artist_follower_breakdown WHERE songstats_id = ? ORDER BY followers DESC", (artist_id,))
        f_rows = c.fetchall()
        print("\n[+] FOLLOWER DISTRIBUTION:")
        for fr in f_rows:
            print(f"  - {fr['platform'].capitalize():<12}: {fr['followers']:,}")

        # 5. Collaborators
        c.execute("SELECT name, collaborator_id FROM artist_collaborators WHERE songstats_id = ?", (artist_id,))
        collabs = c.fetchall()
        print(f"\n[+] COLLABORATORS ({len(collabs)}):")
        for col in collabs:
            print(f"  - {col['name']} (ID: {col['collaborator_id']})")

        # 6. Top Tracks
        c.execute("SELECT rank_order, track_name, popularity_pct, track_id FROM artist_top_tracks WHERE songstats_id = ? ORDER BY rank_order ASC LIMIT 5", (artist_id,))
        tracks = c.fetchall()
        print("\n[+] TOP PERFORMING TRACKS (Top 5):")
        for tr in tracks:
            print(f"  #{tr['rank_order']} {tr['track_name']:<20} Popularity: {tr['popularity_pct']}% (ID: {tr['track_id']})")

        # 7. Recent Highlights
        c.execute("SELECT event_date, source, activity_type, notification_text FROM artist_highlights WHERE songstats_id = ? ORDER BY event_date DESC LIMIT 4", (artist_id,))
        highlights = c.fetchall()
        print("\n[+] RECENT HIGHLIGHTS:")
        for h in highlights:
            print(f"  [{h['event_date']}] ({h['source'].upper()} - {h['activity_type']}): {h['notification_text']}")

        print("\n" + "=" * 75 + "\n")


# =====================================================================
# Main CLI Entrypoint
# =====================================================================

def main():
    parser = argparse.ArgumentParser(description="Songstats Full Dashboard Analytics Scraper")
    parser.add_argument(
        "--artist",
        default=DEFAULT_ARTIST_ID,
        help=f"Target Songstats artist ID or URL (default: {DEFAULT_ARTIST_ID})",
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        help=f"SQLite database file (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--input-dir",
        default=None,
        help="Optional: Directory containing dumped JSON files (e.g. songstats_api_dumps/)",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run browser in visible mode (default is headless)",
    )
    args = parser.parse_args()

    artist_id = extract_artist_id_from_url(args.artist)

    # 1. First check candidate directories for cached files
    search_dirs = []
    if args.input_dir:
        search_dirs.append(Path(args.input_dir))
    search_dirs.extend([Path(DEFAULT_DUMP_DIR), Path(".")])

    data = locate_cached_payloads(search_dirs)

    # 2. If not found locally, execute automated browser fetch
    if data:
        logger.info("Using cached local dashboard payloads.")
    else:
        logger.info(f"Fetching live data via headless browser for artist: {artist_id}...")
        try:
            data = fetch_dashboard_via_browser(artist_id, headless=not args.headed)
        except Exception as exc:
            logger.error(f"Live fetch failed: {exc}")
            sys.exit(1)

    # 3. Persist to SQLite
    save_full_dashboard_to_sqlite(
        account_payload=data["account"],
        top_payload=data["top"],
        bottom_payload=data["bottom"],
        db_path=args.db,
    )

    # 4. Print verified analytics summary
    print_database_summary(artist_id=artist_id, db_path=args.db)


if __name__ == "__main__":
    main()