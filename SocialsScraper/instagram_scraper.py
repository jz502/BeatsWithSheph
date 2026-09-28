import sqlite3
import json
import time
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright

DB_PATH = 'socials_cache.db'

# ==========================================
# TASK 1: DATABASE INITIALIZATION
# ==========================================

def init_db(conn):
    """
    Initializes WAL mode and creates required tables matching the pipeline's exact schema.
    Uses CREATE TABLE IF NOT EXISTS to prevent overwriting existing columns.
    """
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

        # 3. Master Rosetta Stone Table (ensured for clean bootstrapping)
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

def get_instagram_handle(conn, songstats_id):
    """Queries artist_identities dynamically using the provided songstats_id."""
    cursor = conn.cursor()
    cursor.execute("SELECT instagram_handle FROM artist_identities WHERE songstats_id = ?", (songstats_id,))
    row = cursor.fetchone()
    return row[0] if row else None

# ==========================================
# SCRAPING & PAYLOAD NORMALIZATION
# ==========================================

def extract_post_metadata(node):
    """Normalizes individual post items from the GraphQL edge node."""
    post_id = str(node.get("id", ""))
    shortcode = node.get("shortcode", "")
    post_url = f"https://www.instagram.com/p/{shortcode}/" if shortcode else ""
    post_type = node.get("__typename", "GraphImage")
    is_video = bool(node.get("is_video", False))
    display_url = node.get("display_url", "")
    
    caption = ""
    caption_edges = node.get("edge_media_to_caption", {}).get("edges", [])
    if caption_edges and isinstance(caption_edges, list):
        caption = caption_edges[0].get("node", {}).get("text", "")
    elif isinstance(node.get("caption"), str):
        caption = node.get("caption", "")
        
    likes = node.get("edge_media_preview_like", {}).get("count")
    if likes is None:
        likes = node.get("edge_liked_by", {}).get("count", node.get("like_count", 0))

    comments = node.get("edge_media_to_comment", {}).get("count", node.get("comment_count", 0))
    video_views = node.get("video_view_count", node.get("video_play_count", 0)) if is_video else 0
    
    taken_at = node.get("taken_at_timestamp", node.get("taken_at"))
    posted_at = None
    if taken_at:
        try:
            posted_at = datetime.fromtimestamp(int(taken_at), tz=timezone.utc).isoformat()
        except Exception:
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

def extract_metrics_via_interception(handle):
    """Launches headless Playwright with stealth flags to intercept raw JSON responses."""
    metrics = {
        "instagram_followers": 0,
        "instagram_following": 0,
        "instagram_post_count": 0,
        "instagram_recent_likes": 0,
        "instagram_recent_comments": 0
    }
    extracted_posts = []
    flags = {"user_info_received": False, "posts_received": False}
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
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
                
            if "api/v1/instagram/userInfo" in response.url:
                if response.status == 200:
                    try:
                        data = response.json()
                        user_data = data.get("result", [{}])[0].get("user", {})
                        metrics["instagram_followers"] = user_data.get("follower_count", 0)
                        metrics["instagram_following"] = user_data.get("following_count", 0)
                        metrics["instagram_post_count"] = user_data.get("media_count", 0)
                        flags["user_info_received"] = True
                    except Exception as e:
                        print(f"[-] Error parsing userInfo JSON: {e}")

            elif "api/v1/instagram/postsV2" in response.url:
                if response.status == 200:
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
                    except Exception as e:
                        print(f"[-] Error parsing postsV2 JSON: {e}")

        page.on("response", handle_response)

        try:
            print("[-] Loading Picuki SPA...")
            page.goto("https://picuki.site/", wait_until="networkidle", timeout=30000)
            
            print(f"[-] Entering target handle @{handle}...")
            search_input = page.locator("input.main-form__input")
            search_input.wait_for(state="visible", timeout=10000)
            search_input.click()
            search_input.press_sequentially(handle, delay=40)
            time.sleep(0.5)
            
            print("[-] Triggering Search...")
            page.click("button.main-form__field-download")
            
            max_wait_seconds = 14
            start_time = time.time()
            while time.time() - start_time < max_wait_seconds:
                if flags["user_info_received"] and flags["posts_received"]:
                    print("[+] Network payloads intercepted successfully.")
                    break
                page.wait_for_timeout(500)
                
        except Exception as e:
            print(f"[!] Playwright execution error: {e}")
        finally:
            browser.close()
            
    return metrics, extracted_posts

# ==========================================
# TASK 2: SAVE TO CACHE (DELTA DEDUPLICATION)
# ==========================================

def save_to_cache(conn, instagram_handle, current_data):
    """
    Persists data following the pipeline's strict 24-hr TTL and delta ledger pattern.
    Executed entirely within a single atomic transaction.
    """
    timestamp = datetime.now(timezone.utc).isoformat()
    cache_key = f"instagram:{instagram_handle}"
    
    with conn:
        cursor = conn.cursor()
        
        # 1. Query metrics_cache for previous state
        cursor.execute("SELECT data FROM metrics_cache WHERE channel_id = ?", (cache_key,))
        row = cursor.fetchone()
        
        prev_data = {}
        first_run = True
        if row and row[0]:
            try:
                prev_data = json.loads(row[0])
                first_run = False
            except json.JSONDecodeError:
                prev_data = {}

        # 2. Calculate integer metric differences
        deltas = {}
        has_changed = first_run
        
        for key, value in current_data.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                prev_val = prev_data.get(key, 0)
                diff = int(value - prev_val)
                deltas[f"delta_{key}"] = diff
                if diff != 0:
                    has_changed = True
                    
        # 3. Always UPSERT into metrics_cache (Refreshes data & 24-hr TTL updated_at)
        cursor.execute('''
            INSERT INTO metrics_cache (channel_id, data, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET
                data = excluded.data,
                updated_at = excluded.updated_at
        ''', (cache_key, json.dumps(current_data), timestamp))
        print(f"[+] Snapshot updated in 'metrics_cache' for key '{cache_key}'.")

        # 4. Conditional INSERT into historical_metrics (only if changed or first run)
        if has_changed:
            historical_record = dict(current_data)
            historical_record["deltas"] = deltas
            
            cursor.execute('''
                INSERT INTO historical_metrics (platform, entity_id, data, extracted_at)
                VALUES (?, ?, ?, ?)
            ''', ('instagram', instagram_handle, json.dumps(historical_record), timestamp))
            print(f"[+] Variance detected. Delta event appended to 'historical_metrics' for @{instagram_handle}.")
        else:
            print(f"[-] No metric variance detected for @{instagram_handle}. Skipped historical insert.")

def save_posts(conn, songstats_id, instagram_handle, posts):
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
        print(f"[+] Upserted {len(posts)} posts into 'instagram_posts'.")

# ==========================================
# PIPELINE ENTRY POINT
# ==========================================

def process_pipeline(songstats_id="y4ku2hlc"):
    print(f"[*] Starting Instagram Analytics Pipeline for ID: {songstats_id}")
    
    with sqlite3.connect(DB_PATH) as conn:
        init_db(conn)
        
        # Identity lookup from master table
        handle = get_instagram_handle(conn, songstats_id)
        if not handle:
            print(f"[!] No handle found in DB for {songstats_id}. Defaulting to 'k.law_music'.")
            handle = "k.law_music"
            
        print(f"[*] Extracting metrics for @{handle} via Phantom Interceptor...")
        metrics, posts = extract_metrics_via_interception(handle)
        
        if metrics["instagram_followers"] == 0 and metrics["instagram_post_count"] == 0:
            print("[!] Extraction yielded 0. Account may be private or temporarily rate-limited.")
            return

        current_data = {
            "songstats_id": songstats_id,
            "instagram_handle": handle,
            **metrics
        }
        
        print("\n[*] Normalized Current Payload:")
        print(json.dumps(current_data, indent=2))
        
        # Persist data according to pipeline architecture
        save_posts(conn, songstats_id, handle, posts)
        save_to_cache(conn, handle, current_data)
        
    print("[*] Pipeline Execution Complete.")

if __name__ == "__main__":
    process_pipeline("y4ku2hlc")