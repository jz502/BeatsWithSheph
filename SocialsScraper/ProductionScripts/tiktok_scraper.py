#!/usr/bin/env python3
"""
tiktok_scraper.py
-----------------
A production-ready, zero-maintenance TikTok analytics scraper for music pipelines.
Uses Apify's managed extraction to bypass CAPTCHAs, with persistent data storage
in a Docker volume.

Architectural Rules enforced:
1. Default DB path -> data/socials_cache.db
2. Required target handle argument (No hardcoded defaults)
3. Explicit error handling (Fail loudly on missing data/API errors)
4. Standardized stdout logging (No print statements)
5. Strict SQLite WAL schema & Delta Deduplication logic
"""

import argparse
from datetime import datetime, timezone
import json
import logging
import os
import sqlite3
import sys
from typing import Any, Dict, List, Tuple

# Configure logging for Docker (outputs to stdout with timestamps and severity)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("tiktok_scraper")

try:
    from apify_client import ApifyClient
except ImportError:
    logger.error("The 'apify-client' library is required. Install via: pip install apify-client")
    sys.exit(1)

# Strictly read the token from the environment variable
APIFY_API_TOKEN = os.environ.get("APIFY_API_TOKEN")

# Default Production Path for Docker Volumes
DB_PATH = "data/socials_cache.db"


# ============================================================================
# Task 2: Database Schema & Pipeline Alignment
# ============================================================================

def init_db(db_path: str) -> sqlite3.Connection:
    """
    Ensures the data directory exists, connects to SQLite, initializes WAL mode,
    and constructs the required schema.
    """
    # Ensure Docker volume directory exists
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute("PRAGMA journal_mode = WAL;")
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
            CREATE TABLE IF NOT EXISTS tiktok_posts (
                video_id TEXT PRIMARY KEY,
                tiktok_handle TEXT,
                video_url TEXT,
                caption TEXT,
                play_count INTEGER,
                digg_count INTEGER,
                comment_count INTEGER,
                share_count INTEGER,
                duration INTEGER,
                posted_at TIMESTAMP,
                updated_at TIMESTAMP
            );
        """)
    logger.info(f"Database initialized successfully at {db_path} with WAL mode.")
    return conn


# ============================================================================
# Task 1: Scraping Strategy via Apify Managed Extractor
# ============================================================================

def fetch_tiktok_data_apify(
    handle: str, 
    count: int
) -> Tuple[Dict[str, int], List[Dict[str, Any]], Dict[str, int]]:
    """
    Triggers the managed Apify Actor to securely extract TikTok metrics.
    Fails loudly if the API token is missing, the call fails, or data is missing.
    """
    if not APIFY_API_TOKEN:
        logger.error("Security Error: APIFY_API_TOKEN environment variable is missing.")
        sys.exit(1)

    logger.info(f"Dispatching Apify extraction job for @{handle} (Requesting {count} posts)...")
    
    client = ApifyClient(APIFY_API_TOKEN)
    
    run_input = {
        "profiles": [handle],
        "resultsPerPage": count,
        "shouldDownloadVideos": False,
        "shouldDownloadCovers": False,
    }

    try:
        run = client.actor("clockworks/tiktok-profile-scraper").call(run_input=run_input)
    except Exception as e:
        logger.error(f"Apify API call failed catastrophically: {e}")
        raise

    if isinstance(run, dict):
        dataset_id = run.get("defaultDatasetId") or run.get("default_dataset_id")
    else:
        dataset_id = getattr(run, "default_dataset_id", getattr(run, "defaultDatasetId", None))
        
    if not dataset_id:
        raise RuntimeError(f"Failed to retrieve defaultDatasetId from Apify run object: {run}")
    
    dataset_items = list(client.dataset(dataset_id).iterate_items())
    
    if not dataset_items:
        raise RuntimeError(f"Apify returned empty dataset for @{handle}. The handle may be invalid or has no data.")

    # Extract author and video data
    first_item = dataset_items[0]
    author = first_item.get("authorMeta") or first_item
    
    profile_stats = {
        "tiktok_followers": int(author.get("fans", 0)),
        "tiktok_following": int(author.get("following", 0)),
        "tiktok_likes": int(author.get("heart", 0)),
        "tiktok_video_count": int(author.get("video", 0)),
    }

    posts: List[Dict[str, Any]] = []

    for v in dataset_items[:count]:
        video_id = str(v.get("id", ""))
        if not video_id:
            continue

        posted_at = datetime.now(timezone.utc).isoformat()
        if v.get("createTime"):
            try:
                posted_at = datetime.fromtimestamp(int(v["createTime"]), tz=timezone.utc).isoformat()
            except ValueError:
                pass
        
        posts.append({
            "video_id": video_id,
            "video_url": v.get("webVideoUrl", f"https://www.tiktok.com/@{handle}/video/{video_id}"),
            "url": v.get("webVideoUrl", f"https://www.tiktok.com/@{handle}/video/{video_id}"),
            "caption": str(v.get("text", "")).strip(),
            "play_count": int(v.get("playCount", 0)),
            "views": int(v.get("playCount", 0)),
            "digg_count": int(v.get("diggCount", 0)),
            "likes": int(v.get("diggCount", 0)),
            "comment_count": int(v.get("commentCount", 0)),
            "comments": int(v.get("commentCount", 0)),
            "share_count": int(v.get("shareCount", 0)),
            "shares": int(v.get("shareCount", 0)),
            "reposts": int(v.get("shareCount", 0)),
            "duration": int(v.get("videoMeta", {}).get("duration", 0)),
            "posted_at": posted_at,
            "posted_timestamp": posted_at,
        })

    recent_aggregates = {
        "tiktok_recent_views": sum(p["play_count"] for p in posts),
        "tiktok_recent_likes": sum(p["digg_count"] for p in posts),
        "tiktok_recent_comments": sum(p["comment_count"] for p in posts),
        "tiktok_recent_shares": sum(p["share_count"] for p in posts),
        "tiktok_recent_reposts": sum(p["share_count"] for p in posts),
    }

    return profile_stats, posts, recent_aggregates


# ============================================================================
# Task 3: Delta Deduplication & Persistence
# ============================================================================

def save_to_cache(conn: sqlite3.Connection, handle: str, current_data: Dict[str, Any]) -> Dict[str, int]:
    cache_key = f"tiktok:{handle}"
    now_iso = datetime.now(timezone.utc).isoformat()

    with conn:
        cursor = conn.execute("SELECT data FROM metrics_cache WHERE channel_id = ?", (cache_key,))
        row = cursor.fetchone()

        prev_metrics: Dict[str, Any] = {}
        if row and row[0]:
            try:
                parsed_prev = json.loads(row[0])
                if isinstance(parsed_prev, dict):
                    prev_metrics = parsed_prev
            except (json.JSONDecodeError, TypeError):
                pass

        numeric_metrics = {k: int(v) for k, v in current_data.items() if isinstance(v, (int, float)) and k != "deltas"}
        deltas: Dict[str, int] = {}
        has_changed = False

        if not prev_metrics:
            has_changed = True
            for k, val in numeric_metrics.items():
                deltas[k] = val
        else:
            for k, val in numeric_metrics.items():
                prev_val = prev_metrics.get(k, 0)
                diff = int(val - prev_val)
                deltas[k] = diff
                if diff != 0:
                    has_changed = True

        conn.execute(
            """
            INSERT INTO metrics_cache (channel_id, data, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET data = excluded.data, updated_at = excluded.updated_at;
            """,
            (cache_key, json.dumps(current_data), now_iso),
        )

        if has_changed:
            historical_payload = dict(current_data)
            historical_payload["deltas"] = deltas
            conn.execute(
                "INSERT INTO historical_metrics (platform, entity_id, data, extracted_at) VALUES (?, ?, ?, ?);",
                ("tiktok", handle, json.dumps(historical_payload), now_iso),
            )
            logger.info(f"Recorded new historical ledger entry for @{handle}. Deltas: {deltas}")
        else:
            logger.info(f"No numeric metrics changed for @{handle}. Skipped historical ledger insertion.")

    return deltas


def save_posts(conn: sqlite3.Connection, handle: str, posts: List[Dict[str, Any]]) -> None:
    if not posts:
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    with conn:
        conn.executemany(
            """
            INSERT INTO tiktok_posts (
                video_id, tiktok_handle, video_url, caption, play_count, digg_count, 
                comment_count, share_count, duration, posted_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(video_id) DO UPDATE SET
                caption = excluded.caption, play_count = excluded.play_count, 
                digg_count = excluded.digg_count, comment_count = excluded.comment_count, 
                share_count = excluded.share_count, updated_at = excluded.updated_at;
            """,
            [(p["video_id"], handle, p["video_url"], p["caption"], p["play_count"], p["digg_count"], p["comment_count"], p["share_count"], p["duration"], p["posted_at"], now_iso) for p in posts],
        )
    logger.info(f"Successfully upserted {len(posts)} posts for @{handle} into tiktok_posts.")


# ============================================================================
# Main Execution Pipeline
# ============================================================================

def run_scraper(handle: str, db_path: str, post_count: int) -> Dict[str, Any]:
    clean_handle = handle.lstrip("@").strip()
    if not clean_handle:
        raise ValueError("Invalid target handle provided.")
        
    conn = init_db(db_path)

    try:
        profile_stats, posts, recent_aggregates = fetch_tiktok_data_apify(clean_handle, count=post_count)

        current_data = {
            "handle": clean_handle,
            "tiktok_followers": profile_stats.get("tiktok_followers", 0),
            "tiktok_following": profile_stats.get("tiktok_following", 0),
            "tiktok_likes": profile_stats.get("tiktok_likes", 0),
            "tiktok_video_count": profile_stats.get("tiktok_video_count", 0),
            "tiktok_recent_views": recent_aggregates["tiktok_recent_views"],
            "tiktok_recent_likes": recent_aggregates["tiktok_recent_likes"],
            "tiktok_recent_comments": recent_aggregates["tiktok_recent_comments"],
            "tiktok_recent_shares": recent_aggregates["tiktok_recent_shares"],
            "tiktok_recent_reposts": recent_aggregates["tiktok_recent_reposts"],
        }

        deltas = save_to_cache(conn, clean_handle, current_data)
        save_posts(conn, clean_handle, posts)

        logger.info(f"Pipeline finished successfully for @{clean_handle}")
        return {"handle": clean_handle, "metrics": current_data, "deltas": deltas, "posts_count": len(posts)}
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Production TikTok Scraper using Apify (Docker/Portainer Ready).")
    parser.add_argument("handle", type=str, help="The TikTok handle to scrape (Required).")
    parser.add_argument("--db", type=str, default=DB_PATH, help="Path to SQLite database.")
    parser.add_argument("--count", type=int, default=12, help="Number of posts to fetch.")
    
    args = parser.parse_args()

    try:
        summary = run_scraper(args.handle, args.db, args.count)
        
        # Standardized JSON logging for orchestrator consumption
        logger.info(f"=== SCRAPE SUMMARY FOR @{summary['handle']} ===")
        logger.info(f"Metrics Payload:\n{json.dumps(summary['metrics'], indent=2)}")
        logger.info(f"Deltas Recorded:\n{json.dumps(summary['deltas'], indent=2)}")
        logger.info(f"Total posts captured: {summary['posts_count']}")
        
    except Exception as exc:
        logger.error(f"Scraper execution catastrophically failed: {exc}", exc_info=True)
        sys.exit(1)