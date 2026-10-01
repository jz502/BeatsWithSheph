To transition this project from a collection of development scripts into a **production-ready, containerized pipeline**, we need to implement a strict architectural standard. 

By unifying around `songstats_id` as the primary identifier, enforcing strict error handling (failing loudly instead of silently returning default data), and containerizing via Portainer, you will create a rock-solid backend that can seamlessly feed into a future dashboard.

Here is the complete blueprint to polish the scripts, set up your server, and orchestrate the system.

---

### Phase 1: Server Directory & Environment Setup

On your Linux server, create a master directory for this project. This will house all your scripts, Docker configs, and the persistent SQLite database.

```bash
mkdir -p /opt/music-analytics/data
cd /opt/music-analytics
```
*Note: The `data/` subdirectory is crucial. This is the mapped volume where your `socials_cache.db` will live so it isn't destroyed when the Docker container restarts.*

Create your `requirements.txt` in this directory:
```text
curl_cffi==0.7.1
playwright==1.42.0
apify-client==1.6.2
schedule==1.2.1
```

---

### Phase 2: Script Polish & Standardization

You must apply these **three strict rules** to all of your existing scraper scripts (`spotify_scraper.py`, `youtube_scraper.py`, `instagram_scraper.py`, `tiktok_scraper.py`, `songstats_spotify_scraper.py`, `songstats_scraper.py`):

1. **Update the Database Path:**
   Change the `DB_PATH` in every script to point to the Docker volume:
   ```python
   DB_PATH = "data/socials_cache.db"
   ```
2. **Remove Hardcoded Defaults:**
   Remove all fallback/default IDs (like K.LAW's `y4ku2hlc`). The scripts must require an argument and fail if one isn't provided.
   ```python
   # INSTEAD OF THIS:
   target_id = sys.argv[1] if len(sys.argv) > 1 else "y4ku2hlc"
   
   # DO THIS:
   import argparse
   parser = argparse.ArgumentParser()
   parser.add_argument("target_id", help="Target ID to scrape")
   args = parser.parse_args()
   ```
3. **Explicit Error Handling (No silent zero-returns):**
   If an extraction fails (e.g., account is banned, network timeout), the script must `raise Exception` and exit with a non-zero code. Do *not* return `{ "followers": 0 }`.
   ```python
   if resp.status_code != 200:
       raise RuntimeError(f"Failed to fetch data: HTTP {resp.status_code}")
       sys.exit(1)
   ```

---

### Phase 3: The New Core Modules

To meet your requirements of (A) using `songstats_id` as the primary key and (B) allowing manual overrides for missing socials (like K.LAW's TikTok), we need two new core scripts.

#### 1. The Artist Manager (`manage_artist.py`)
This script allows you to onboard artists, fetch their initial Songstats IDs, and manually update any missing platform links directly in the database. Save this as `manage_artist.py`.

```python
#!/usr/bin/env python3
import sqlite3
import argparse
import sys

DB_PATH = "data/socials_cache.db"

def init_db(conn):
    conn.execute("PRAGMA journal_mode = WAL;")
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS artist_identities (
                songstats_id TEXT PRIMARY KEY,
                artist_name TEXT,
                spotify_id TEXT,
                youtube_id TEXT,
                instagram_handle TEXT,
                tiktok_handle TEXT,
                soundcloud_handle TEXT,
                updated_at TIMESTAMP
            )
        """)

def add_or_update_artist(args):
    with sqlite3.connect(DB_PATH) as conn:
        init_db(conn)
        
        # Check if artist exists
        cur = conn.execute("SELECT * FROM artist_identities WHERE songstats_id = ?", (args.songstats_id,))
        exists = cur.fetchone()

        if exists:
            # Update specific column
            if not args.platform or not args.value:
                print("Error: To update an existing artist, provide --platform and --value.")
                sys.exit(1)
            
            allowed_platforms = ['artist_name', 'spotify_id', 'youtube_id', 'instagram_handle', 'tiktok_handle', 'soundcloud_handle']
            if args.platform not in allowed_platforms:
                print(f"Error: Platform must be one of {allowed_platforms}")
                sys.exit(1)

            conn.execute(f"UPDATE artist_identities SET {args.platform} = ?, updated_at = CURRENT_TIMESTAMP WHERE songstats_id = ?", (args.value, args.songstats_id))
            print(f"[SUCCESS] Updated {args.platform} for {args.songstats_id} to '{args.value}'.")
        else:
            # Create new baseline
            if not args.name:
                print("Error: --name is required when adding a new artist.")
                sys.exit(1)
            conn.execute("""
                INSERT INTO artist_identities (songstats_id, artist_name, updated_at) 
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (args.songstats_id, args.name))
            print(f"[SUCCESS] Added new artist: {args.name} ({args.songstats_id}). Run songstats_scraper to auto-fill platforms, or update them manually.")

def list_artists():
    with sqlite3.connect(DB_PATH) as conn:
        init_db(conn)
        cur = conn.execute("SELECT songstats_id, artist_name, spotify_id, youtube_id, instagram_handle, tiktok_handle FROM artist_identities")
        print(f"{'Songstats ID':<15} | {'Name':<20} | {'Spotify':<25} | {'YouTube':<25} | {'Instagram':<15} | {'TikTok':<15}")
        print("-" * 125)
        for row in cur.fetchall():
            print(f"{str(row[0]):<15} | {str(row[1]):<20} | {str(row[2]):<25} | {str(row[3]):<25} | {str(row[4]):<15} | {str(row[5]):<15}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage the Artist Identity Graph database.")
    subparsers = parser.add_subparsers(dest="command")

    parser_add = subparsers.add_parser("update", help="Add or update an artist's identity.")
    parser_add.add_argument("songstats_id", help="The primary Songstats ID (e.g., y4ku2hlc)")
    parser_add.add_argument("--name", help="Artist Name (required if new)")
    parser_add.add_argument("--platform", help="Platform column to update (e.g., tiktok_handle)")
    parser_add.add_argument("--value", help="The ID or Handle to set")

    parser_list = subparsers.add_parser("list", help="List all tracked artists.")

    args = parser.parse_args()

    if args.command == "update":
        add_or_update_artist(args)
    elif args.command == "list":
        list_artists()
    else:
        parser.print_help()
```
*(Usage Example: `python manage_artist.py update y4ku2hlc --platform tiktok_handle --value k.law__`)*

#### 2. The Production Orchestrator (`orchestrator.py`)
This script runs endlessly inside Docker. Every day at 3:00 AM, it queries `artist_identities` (using `songstats_id` as the anchor) and triggers the scrapers.

```python
#!/usr/bin/env python3
import sqlite3
import subprocess
import time
import logging
import sys
import schedule

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ORCHESTRATOR] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

DB_PATH = "data/socials_cache.db"

def run_scraper(command: list, platform_name: str, artist_name: str):
    logging.info(f"Starting {platform_name} extraction for {artist_name}...")
    try:
        # We use subprocess to guarantee complete isolation of dependencies (like Playwright contexts)
        result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            logging.info(f"[SUCCESS] {platform_name} | {artist_name}")
        else:
            logging.error(f"[FAILED] {platform_name} | {artist_name} | Error: {result.stderr.strip()}")
            
    except subprocess.TimeoutExpired:
        logging.error(f"[TIMEOUT] {platform_name} | {artist_name} exceeded 5 minutes.")
    except Exception as e:
        logging.error(f"[ERROR] {platform_name} | {artist_name} | {e}")

def pipeline_job():
    logging.info("=== STARTING DAILY ANALYTICS PIPELINE ===")
    
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT songstats_id, artist_name, youtube_id, spotify_id, instagram_handle, tiktok_handle 
                FROM artist_identities
            """)
            artists = cursor.fetchall()
    except sqlite3.OperationalError as e:
        logging.error(f"Database error (Is it initialized?): {e}")
        return

    if not artists:
        logging.warning("No artists found in 'artist_identities'. Please add artists via manage_artist.py.")
        return

    for artist in artists:
        songstats_id, artist_name, youtube_id, spotify_id, instagram_handle, tiktok_handle = artist
        logging.info(f"--- Processing Artist: {artist_name} ({songstats_id}) ---")
        
        if youtube_id:
            run_scraper(["python", "youtube_scraper.py", youtube_id], "YouTube", artist_name)
        if spotify_id:
            run_scraper(["python", "spotify_scraper.py", spotify_id], "Spotify", artist_name)
        if songstats_id:
            run_scraper(["python", "songstats_spotify_scraper.py", songstats_id], "Songstats Spotify", artist_name)
        if instagram_handle:
            run_scraper(["python", "instagram_scraper.py", instagram_handle], "Instagram", artist_name)
        if tiktok_handle:
            run_scraper(["python", "tiktok_scraper.py", tiktok_handle], "TikTok", artist_name)

        logging.info(f"Finished {artist_name}. Resting 15s to respect API rate limits...")
        time.sleep(15)

    logging.info("=== DAILY ANALYTICS PIPELINE COMPLETE ===")

if __name__ == "__main__":
    logging.info("Orchestrator container started.")
    
    # Run once on boot to verify health
    pipeline_job()

    # Schedule for 3:00 AM Daily
    schedule.every().day.at("03:00").do(pipeline_job)
    logging.info("Scheduler active. Waiting for 3:00 AM.")
    
    while True:
        schedule.run_pending()
        time.sleep(60)
```

---

### Phase 4: Portainer & Docker Deployment

To host this gracefully on your Linux server, create this `Dockerfile` in your project folder:

```dockerfile
FROM mcr.microsoft.com/playwright/python:v1.42.0-jammy

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all python files
COPY *.py .

# Ensure Playwright browsers are installed
RUN playwright install chromium

CMD ["python", "-u", "orchestrator.py"]
```

Create this `docker-compose.yml`:
```yaml
version: '3.8'

services:
  analytics-engine:
    build: .
    container_name: music-analytics-engine
    restart: unless-stopped
    volumes:
      - ./data:/app/data
    environment:
      - YOUTUBE_API_KEY=${YOUTUBE_API_KEY}
      - APIFY_API_TOKEN=${APIFY_API_TOKEN}
      - TZ=Australia/Sydney
```

**To Deploy via Portainer:**
1. Open Portainer on your server.
2. Go to **Stacks** > **Add stack**.
3. Name it `music-analytics`.
4. Choose **Web editor** and paste the `docker-compose.yml` contents.
5. In the **Environment variables** section at the bottom, add:
   * Name: `YOUTUBE_API_KEY`, Value: `your-key`
   * Name: `APIFY_API_TOKEN`, Value: `your-token`
6. Since Portainer defaults to pulling images, and we are *building* an image locally from the `/opt/music-analytics` folder, it is actually easier to build it via terminal first, OR point Portainer's stack to your absolute server path.
   * *Alternative/Easier way for local files:* Run `docker-compose up -d --build` directly in the terminal from your `/opt/music-analytics` folder. Once built, it will automatically appear in Portainer where you can manage it, view logs, and stop/start it using the UI.

---

### Phase 5: Managing the System (Prep for the Dashboard)

Before building a visual dashboard, you interact with the system using your CLI tool. 

If you want to track a new artist (e.g., K.LAW):
1. Shell into your server (or use Portainer's Console).
2. Run: `python manage_artist.py update y4ku2hlc --name "K.LAW"`
3. Run: `python songstats_scraper.py https://songstats.com/artist/y4ku2hlc/k-law` (This will auto-fill Spotify, YT, IG).
4. Run: `python manage_artist.py update y4ku2hlc --platform tiktok_handle --value k.law__` (Manual override for TikTok).

From this point on, every day at 3:00 AM, the orchestrator will pull K.LAW's `songstats_id` from the database, distribute the unique handles to the scrapers, calculate the exact numeric deltas, and append them to `historical_metrics`. 

### The Path to a Dashboard
Because your entire pipeline is writing perfectly normalized data to an isolated SQLite `data/socials_cache.db` file, building a dashboard is incredibly easy. 
For your next step, you have two great options:
1. **Zero-Code:** Deploy a **Metabase** Docker container in Portainer, point it to your `data/socials_cache.db`, and you can instantly build drag-and-drop graphs of your `historical_metrics`.
2. **Custom Web App:** Build a lightweight **Streamlit** or **Next.js** frontend that reads the SQLite database and displays the data exactly how your agency/team needs it. 

Which dashboard approach would you prefer to explore next?