#!/usr/bin/env python3
"""
Production Instagram Scraper Module
Part of the Local Multi-Platform Music Analytics Pipeline.

Extracts public vanity metrics and recent post data via Playwright
network interception and persists them to SQLite in WAL mode with delta tracking.
"""

import os
import sys
import json
import time
import logging
import sqlite3
import argparse
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ==========================================
# RULE 1: PERSISTENT STORAGE PATH
# ==========================================
DB_PATH = "data/socials_cache.db"

# ==========================================
# RULE 4: STANDARDIZED DOCKER LOGGING
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("instagram_scraper")


# ==========================================
# DATABASE INITIALIZATION & LOOKUP
# ==========================================

def init_db(conn: sqlite3.Connection) -> None:
    """Configures WAL mode and initializes standard pipeline tables."""
    conn.execute("PRAGMA journal_mode = WAL;")
    with conn:
        # 1. 24-Hour TTL Snapshot Table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS metrics_cache (
                channel_id TEXT PRIMARY KEY,
                data TEXT,
                updated_at TIMESTAMP
            )
        ''')

        # 2. Time-Series Event Ledger
        conn.execute('''
            CREATE TABLE IF NOT EXISTS historical_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT,
                entity_id TEXT,
                data TEXT,
                extracted_at TIMESTAMP
            )
        ''')

        # 3. Master Identity Table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS artist_identities (
                songstats_id TEXT PRIMARY KEY,
                artist_name TEXT,
                spotify_id TEXT,
                youtube_id TEXT,
                instagram_handle TEXT,
                soundcloud_handle TEXT,
                updated_at TIMESTAMP
            )
        ''')

        # 4. Granular Posts Table
        conn.execute('''
            CREATE TABLE IF NOT EXISTS instagram_posts (
                post_id TEXT PRIMARY KEY,
                songstats_id TEXT,
                instagram_handle TEXT,
                shortcode TEXT,
                post_url TEXT,
                post_type TEXT,
                is_video INTEGER,
                caption TEXT,
                likes_count INTEGER,
                comments_count INTEGER,
                video_views INTEGER,
                display_url TEXT,
                posted_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        ''')


def get_instagram_handle(conn: sqlite3.Connection, songstats_id: str) -> str:
    """Queries artist_identities dynamically using the provided songstats_id."""
    cursor = conn.cursor()
    cursor.execute("SELECT instagram_handle FROM artist_identities WHERE songstats_id = ?", (songstats_id,))
    row = cursor.fetchone()
    
    if not row or not row[0]:
        raise ValueError(f"No instagram_handle mapped to songstats_id '{songstats_id}' in artist_identities.")
    return row[0].strip()


# ==========================================
# EXTRACTION & PARSING
# ==========================================

def extract_post_metadata(node: dict) -> dict:
    """Normalizes an individual post node from the intercepted GraphQL payload."""
    post_id = str(node.get("id", ""))
    if not post_id:
        raise ValueError("Encountered post edge without an 'id' attribute.")

    shortcode = node.get("shortcode", "")
    post_url = f"https://www.instagram.com/p/{shortcode}/" if shortcode else ""
    post_type = node.get("__typename", "GraphImage")
    is_video = bool(node.get("is_video", False))
    display_url = node.get("display_url", "")
    
    # Caption extraction
    caption = ""
    caption_edges = node.get("edge_media_to_caption", {}).get("edges", [])
    if caption_edges and isinstance(caption_edges, list):
        caption = caption_edges[0].get("node", {}).get("text", "")
    elif isinstance(node.get("caption"), str):
        caption = node.get("caption", "")
        
    # Likes & comments
    likes = node.get("edge_media_preview_like", {}).get("count")
    if likes is None:
        likes = node.get("edge_liked_by", {}).get("count", node.get("like_count", 0))

    comments = node.get("edge_media_to_comment", {}).get("count", node.get("comment_count", 0))
    video_views = node.get("video_view_count", node.get("video_play_count", 0)) if is_video else 0
    
    # Timestamp parsing
    taken_at = node.get("taken_at_timestamp", node.get("taken_at"))
    posted_at = None
    if taken_at:
        try:
            posted_at = datetime.fromtimestamp(int(taken_at), tz=timezone.utc).isoformat()
        except Exception as e:
            logger.warning(f"Could not parse timestamp '{taken_at}': {e}")
            posted_at = str(taken_at)

    return {
        "post_id": post_id,
        "shortcode": shortcode,
        "post_url": post_url,
        "post_type": post_type,
        "is_video": 1 if is_video else 0,
        "caption": caption,
        "likes_count": int(likes or 0),
        "comments_count": int(comments or 0),
        "video_views": int(video_views or 0),
        "display_url": display_url,
        "posted_at": posted_at
    }


def extract_metrics_via_interception(handle: str) -> tuple[dict, list[dict]]:
    """
    Automates Picuki's Vue SPA using headless Chromium and intercepts raw JSON payloads.
    Fails loudly on non-200 HTTP responses, parsing failures, or timeouts.
    """
    metrics = {}
    extracted_posts = []
    flags = {"user_info_received": False, "posts_received": False}
    critical_errors = []

    logger.info(f"Initializing headless browser session for @{handle}...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage"
            ]
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        def handle_response(response):
            if response.request.method != "POST":
                return
                
            # 1. Profile Level Metrics
            if "api/v1/instagram/userInfo" in response.url:
                if response.status != 200:
                    critical_errors.append(f"userInfo endpoint returned HTTP {response.status}: {response.text()[:200]}")
                    return

                try:
                    data = response.json()
                    user_list = data.get("result", [])
                    if not user_list or not isinstance(user_list, list):
                        critical_errors.append("Invalid userInfo JSON payload structure: 'result' array missing.")
                        return

                    user_data = user_list[0].get("user", {})
                    metrics["instagram_followers"] = user_data["follower_count"]
                    metrics["instagram_following"] = user_data["following_count"]
                    metrics["instagram_post_count"] = user_data["media_count"]
                    flags["user_info_received"] = True
                    logger.info("Successfully intercepted and parsed 'userInfo' payload.")
                except KeyError as ke:
                    critical_errors.append(f"Missing core metric field in userInfo payload: {ke}")
                except Exception as e:
                    critical_errors.append(f"Failed to decode userInfo JSON: {e}")

            # 2. Individual Posts & Aggregate Engagement
            elif "api/v1/instagram/postsV2" in response.url:
                if response.status != 200:
                    critical_errors.append(f"postsV2 endpoint returned HTTP {response.status}: {response.text()[:200]}")
                    return

                try:
                    data = response.json()
                    edges = data.get("result", {}).get("edges", [])
                    
                    tot_likes, tot_comments = 0, 0
                    for edge in edges:
                        node = edge.get("node", {})
                        post_data = extract_post_metadata(node)
                        extracted_posts.append(post_data)
                        tot_likes += post_data["likes_count"]
                        tot_comments += post_data["comments_count"]
                        
                    metrics["instagram_recent_likes"] = tot_likes
                    metrics["instagram_recent_comments"] = tot_comments
                    flags["posts_received"] = True
                    logger.info(f"Successfully intercepted 'postsV2' payload ({len(extracted_posts)} posts parsed).")
                except Exception as e:
                    critical_errors.append(f"Failed to decode postsV2 JSON: {e}")

        page.on("response", handle_response)

        try:
            logger.info("Loading anonymous mirror SPA...")
            page.goto("https://picuki.site/", wait_until="networkidle", timeout=30000)
            
            logger.info(f"Submitting target handle: @{handle}")
            search_input = page.locator("input.main-form__input")
            search_input.wait_for(state="visible", timeout=10000)
            search_input.click()
            
            # Press sequentially to trigger reactive Vue event listeners
            search_input.press_sequentially(handle, delay=35)
            time.sleep(0.5)
            page.click("button.main-form__field-download")
            
            # Await background API resolution
            max_wait_seconds = 15
            start_time = time.time()
            while time.time() - start_time < max_wait_seconds:
                if flags["user_info_received"] and flags["posts_received"]:
                    break
                if critical_errors:
                    raise RuntimeError(f"Critical API error intercepted: {critical_errors[0]}")
                page.wait_for_timeout(500)
                
        except PlaywrightTimeoutError as te:
            raise TimeoutError(f"Playwright navigation timed out for @{handle}: {te}") from te
        finally:
            browser.close()

    # Rule 3: Fail Loudly if any network/data issues occurred
    if critical_errors:
        raise RuntimeError(f"Extraction halted due to API error: {critical_errors[0]}")
        
    if not (flags["user_info_received"] and flags["posts_received"]):
        raise RuntimeError(
            f"Extraction failed for @{handle}. Target API payloads were not intercepted "
            f"(userInfo: {flags['user_info_received']}, postsV2: {flags['posts_received']}). "
            "Account may be private, non-existent, or rate-limited."
        )

    return metrics, extracted_posts


# ==========================================
# RULE 5: PERSISTENCE & DELTA DEDUPLICATION
# ==========================================

def save_to_cache(conn: sqlite3.Connection, instagram_handle: str, current_data: dict) -> None:
    """Persists data to metrics_cache and conditionally appends to historical_metrics."""
    timestamp = datetime.now(timezone.utc).isoformat()
    cache_key = f"instagram:{instagram_handle}"
    
    with conn:
        cursor = conn.cursor()
        
        # 1. Query previous snapshot
        cursor.execute("SELECT data FROM metrics_cache WHERE channel_id = ?", (cache_key,))
        row = cursor.fetchone()
        
        prev_data = {}
        first_run = True
        if row and row[0]:
            try:
                prev_data = json.loads(row[0])
                first_run = False
            except json.JSONDecodeError:
                logger.warning(f"Corrupted cache JSON detected for {cache_key}. Rebuilding snapshot.")
                prev_data = {}

        # 2. Calculate integer metric deltas
        deltas = {}
        has_changed = first_run
        
        for key, value in current_data.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                prev_val = prev_data.get(key, 0)
                diff = int(value - prev_val)
                deltas[f"delta_{key}"] = diff
                if diff != 0:
                    has_changed = True
                    
        # 3. Always update snapshot cache (refreshes 24-hr TTL)
        cursor.execute('''
            INSERT INTO metrics_cache (channel_id, data, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET
                data = excluded.data,
                updated_at = excluded.updated_at
        ''', (cache_key, json.dumps(current_data), timestamp))
        logger.info(f"Refreshed snapshot cache for '{cache_key}'.")

        # 4. Conditional insert into historical ledger
        if has_changed:
            historical_record = dict(current_data)
            historical_record["deltas"] = deltas
            
            cursor.execute('''
                INSERT INTO historical_metrics (platform, entity_id, data, extracted_at)
                VALUES (?, ?, ?, ?)
            ''', ('instagram', instagram_handle, json.dumps(historical_record), timestamp))
            logger.info(f"Variance detected. Appended new delta entry into historical_metrics for @{instagram_handle}.")
        else:
            logger.info(f"No metric variance detected for @{instagram_handle}. Skipped historical ledger insertion.")


def save_posts(conn: sqlite3.Connection, songstats_id: str | None, instagram_handle: str, posts: list[dict]) -> None:
    """Upserts individual post metadata and engagement metrics."""
    if not posts:
        return
        
    timestamp = datetime.now(timezone.utc).isoformat()
    with conn:
        cursor = conn.cursor()
        for post in posts:
            cursor.execute('''
                INSERT INTO instagram_posts (
                    post_id, songstats_id, instagram_handle, shortcode, post_url,
                    post_type, is_video, caption, likes_count, comments_count,
                    video_views, display_url, posted_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(post_id) DO UPDATE SET
                    likes_count = excluded.likes_count,
                    comments_count = excluded.comments_count,
                    video_views = excluded.video_views,
                    caption = excluded.caption,
                    display_url = excluded.display_url,
                    updated_at = excluded.updated_at
            ''', (
                post["post_id"], songstats_id, instagram_handle, post["shortcode"], post["post_url"],
                post["post_type"], post["is_video"], post["caption"], post["likes_count"],
                post["comments_count"], post["video_views"], post["display_url"],
                post["posted_at"], timestamp
            ))
        logger.info(f"Upserted {len(posts)} posts into 'instagram_posts'.")


# ==========================================
# PIPELINE ORCHESTRATION ENTRY POINT
# ==========================================

def run(songstats_id: str | None = None, handle: str | None = None) -> None:
    """Main execution controller."""
    # Ensure database directory exists inside the container volume
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    try:
        conn = sqlite3.connect(DB_PATH)
    except sqlite3.Error as e:
        logger.critical(f"Failed to connect to SQLite database at '{DB_PATH}': {e}")
        sys.exit(1)

    try:
        init_db(conn)

        # Resolve handle dynamically
        if songstats_id:
            logger.info(f"Resolving Instagram handle from DB for songstats_id '{songstats_id}'...")
            resolved_handle = get_instagram_handle(conn, songstats_id)
        elif handle:
            resolved_handle = handle.strip().lstrip("@")
        else:
            raise ValueError("Execution requires either --songstats-id or --handle.")

        logger.info(f"Executing extraction pipeline for @{resolved_handle}...")
        metrics, posts = extract_metrics_via_interception(resolved_handle)

        current_data = {
            "songstats_id": songstats_id,
            "instagram_handle": resolved_handle,
            **metrics
        }
        
        logger.info(f"Extracted payload: {json.dumps(current_data)}")

        # Persist data
        save_posts(conn, songstats_id, resolved_handle, posts)
        save_to_cache(conn, resolved_handle, current_data)
        
        logger.info(f"Pipeline successfully completed for @{resolved_handle}.")
        
    except Exception as e:
        logger.error(f"Pipeline execution aborted due to unrecoverable error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        conn.close()


def main():
    """Rule 2: Command-Line Argument Parsing."""
    parser = argparse.ArgumentParser(
        description="Production Instagram Scraper Module for Music Analytics Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--songstats-id", "-s",
        type=str,
        help="Songstats ID used to dynamically resolve the artist's Instagram handle via artist_identities"
    )
    group.add_argument(
        "--handle", "-u",
        type=str,
        help="Target Instagram handle (bypasses artist_identities lookup)"
    )

    args = parser.parse_args()
    run(songstats_id=args.songstats_id, handle=args.handle)


if __name__ == "__main__":
    main()