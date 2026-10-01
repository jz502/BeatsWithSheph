#!/usr/bin/env python3
import os
import sqlite3
import subprocess
import time
import logging
import sys
import schedule
import random
from datetime import datetime, timezone

DB_PATH = "data/socials_cache.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ORCHESTRATOR] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode = WAL;")
        # Job Queue Table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS job_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                artist_id TEXT,
                platform TEXT,
                job_type TEXT, -- 'manual' or 'scheduled'
                status TEXT DEFAULT 'pending', -- pending, running, completed, failed
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            )
        """)
        # Script Health Table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS script_health (
                artist_id TEXT,
                platform TEXT,
                last_run TIMESTAMP,
                status TEXT,
                last_error TEXT,
                PRIMARY KEY (artist_id, platform)
            )
        """)

def update_health(artist_id, platform, status, error_msg=""):
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO script_health (artist_id, platform, last_run, status, last_error)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(artist_id, platform) DO UPDATE SET
                last_run = excluded.last_run,
                status = excluded.status,
                last_error = excluded.last_error
        """, (artist_id, platform, now, status, error_msg))

def run_scraper(artist_id, platform, job_type):
    
    """Executes the correct script based on platform."""
    # Map platforms to their scripts and arguments
    script_map = {
        "songstats": ["python", "songstats_scraper.py", "--artist", artist_id],
        "youtube": ["python", "youtube_scraper.py", ""], # Will fetch ID from DB
        "spotify": ["python", "spotify_scraper.py", ""],
        "songstats_spotify": ["python", "songstats_spotify_scraper.py", artist_id],
        "instagram": ["python", "instagram_scraper.py", "--songstats-id", artist_id],
        "tiktok": ["python", "tiktok_scraper.py", ""]
    }
    
    if platform not in script_map:
        return False, "Unknown platform"

    cmd = script_map[platform]

    # Resolve platform IDs from songstats_id
    if platform in ["youtube", "spotify", "tiktok"]:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.execute("SELECT youtube_id, spotify_id, tiktok_handle FROM artist_identities WHERE songstats_id = ?", (artist_id,))
            row = cur.fetchone()
            if not row:
                return False, "Artist identity not found"
            
            if platform == "youtube":
                if not row[0]: return False, "No YouTube ID mapped."
                cmd[2] = row[0]
            elif platform == "spotify":
                if not row[1]: return False, "No Spotify ID mapped."
                cmd[2] = row[1]
            elif platform == "tiktok":
                if not row[2]: return False, "No TikTok handle mapped."
                cmd[2] = row[2]

    logging.info(f"Executing {platform} for {artist_id}...")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode == 0:
            logging.info(f"[SUCCESS] {platform} | {artist_id}")
            return True, "Success"
        else:
            err = result.stderr.strip()[-200:] # Capture last 200 chars of error
            logging.error(f"[FAILED] {platform} | {artist_id} | {err}")
            return False, err
    except subprocess.TimeoutExpired:
        return False, "Timeout > 5 mins"
    except Exception as e:
        return False, str(e)
def queue_daily_jobs():
    """Triggered at 3 AM. Queues all tracked platforms for all artists."""
    logging.info("=== QUEUING DAILY SCHEDULED JOBS ===")
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("SELECT songstats_id FROM artist_identities")
        artists = [r[0] for r in cur.fetchall()]
        
        platforms = ["songstats", "youtube", "spotify", "songstats_spotify", "instagram", "tiktok"]
        for artist in artists:
            for plat in platforms:
                conn.execute(
                    "INSERT INTO job_queue (artist_id, platform, job_type) VALUES (?, ?, 'scheduled')",
                    (artist, plat)
                )

def process_queue():
    """Polls the queue and runs pending jobs one by one."""
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("SELECT id, artist_id, platform, job_type FROM job_queue WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1")
        job = cur.fetchone()
    
    if not job:
        return # Queue is empty

    job_id, artist_id, platform, job_type = job

    # Mark as running
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE job_queue SET status = 'running' WHERE id = ?", (job_id,))

    # Execute
    success, msg = run_scraper(artist_id, platform, job_type)
    
    # Finalize state
    status_str = 'completed' if success else 'failed'
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE job_queue SET status = ?, completed_at = ? WHERE id = ?", (status_str, now, job_id))
    
    update_health(artist_id, platform, status_str, msg)

    # ANTI-BOT STAGGERING:
    # If it was a scheduled job, wait between 30 to 90 seconds to avoid tripping bot alarms.
    # Manual jobs bypass this delay for better UX.
    if job_type == 'scheduled':
        delay = random.uniform(30, 90)
        logging.info(f"Anti-Bot: Staggering next script for {int(delay)} seconds...")
        time.sleep(delay)

if __name__ == "__main__":
    init_db()
    logging.info("Orchestrator online. Engine is listening to the job queue.")
    
    schedule.every().day.at("03:00").do(queue_daily_jobs)
    
    while True:
        schedule.run_pending()
        process_queue()
        time.sleep(5) # Poll queue every 5 seconds