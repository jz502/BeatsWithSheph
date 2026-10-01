#!/usr/bin/env python3
"""
Production YouTube & SocialBlade Analytics Ingestion Module
Designed for containerized execution and orchestration via Docker/Portainer.
"""

import argparse
from datetime import datetime, timezone
import json
import logging
import os
import re
import sqlite3
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request

# -----------------------------------------------------------------------------
# Configuration & Constants (Rule 1 & Rule 4)
# -----------------------------------------------------------------------------
DB_PATH = "data/socials_cache.db"
TTL_SECONDS = 86400

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [youtube_scraper]: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("youtube_scraper")

try:
    import certifi
    DEFAULT_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except Exception:
    try:
        DEFAULT_SSL_CONTEXT = ssl.create_default_context()
    except Exception:
        DEFAULT_SSL_CONTEXT = ssl._create_unverified_context()

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Ch-Ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

TRACKED_METRIC_KEYS = [
    "youtube_subscribers",
    "youtube_total_views",
    "youtube_video_count",
    "youtube_recent_50_views",
    "youtube_recent_50_likes",
    "youtube_recent_50_comments",
    "sb_subscribers_last_30_days",
    "sb_views_last_30_days",
    "sb_subscribers_last_14_days",
    "sb_views_last_14_days",
    "sb_daily_avg_views",
    "sb_monthly_earnings_min",
    "sb_monthly_earnings_max",
    "sb_yearly_earnings_min",
    "sb_yearly_earnings_max",
]


# -----------------------------------------------------------------------------
# Database Management (Rule 1 & Rule 5)
# -----------------------------------------------------------------------------
def init_db():
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")

    # Current snapshot cache table (TTL verification)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metrics_cache (
            channel_id TEXT PRIMARY KEY,
            data JSON NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    # Append-only time-series table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS historical_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            platform TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            data JSON NOT NULL,
            extracted_at TEXT NOT NULL
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_historical_platform_entity_extracted
        ON historical_metrics (platform, entity_id, extracted_at DESC)
    """)

    conn.commit()
    return conn


def get_cached_data(conn, channel_id: str):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT data, updated_at FROM metrics_cache WHERE channel_id = ?",
        (channel_id,),
    )
    row = cursor.fetchone()
    if row:
        data_raw, updated_at_str = row
        try:
            cached_dt = datetime.fromisoformat(updated_at_str)
            if cached_dt.tzinfo is None:
                cached_dt = cached_dt.replace(tzinfo=timezone.utc)
            age_seconds = (datetime.now(timezone.utc) - cached_dt).total_seconds()
        except (ValueError, TypeError):
            try:
                age_seconds = time.time() - float(updated_at_str)
            except Exception:
                return None

        if age_seconds < TTL_SECONDS:
            logger.info(
                "Fresh cache entry found for channel %s (age: %.1f hours).",
                channel_id,
                age_seconds / 3600,
            )
            return json.loads(data_raw)
    return None


def save_to_cache(conn, channel_id: str, data: dict):
    now_iso = datetime.now(timezone.utc).isoformat()
    cursor = conn.cursor()

    with conn:
        cursor.execute(
            "SELECT data FROM metrics_cache WHERE channel_id = ?",
            (channel_id,),
        )
        row = cursor.fetchone()
        prev_data = json.loads(row[0]) if row and row[0] else None

        deltas = {}
        has_changes = False

        if prev_data is None:
            has_changes = True
            for key in TRACKED_METRIC_KEYS:
                val = data.get(key)
                deltas[f"delta_{key}"] = int(val) if isinstance(val, (int, float)) else 0
        else:
            for key in TRACKED_METRIC_KEYS:
                new_val = data.get(key)
                old_val = prev_data.get(key)

                new_int = int(new_val) if isinstance(new_val, (int, float)) else 0
                old_int = int(old_val) if isinstance(old_val, (int, float)) else 0

                delta = new_int - old_int
                deltas[f"delta_{key}"] = delta

                if delta != 0:
                    has_changes = True

        # Action A: Upsert current state for TTL enforcement
        cursor.execute("""
            INSERT INTO metrics_cache (channel_id, data, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET
                data = excluded.data,
                updated_at = excluded.updated_at
        """, (channel_id, json.dumps(data), now_iso))

        # Action B: Conditional append to historical_metrics
        if has_changes:
            historical_payload = {
                **data,
                "deltas": deltas,
            }
            cursor.execute("""
                INSERT INTO historical_metrics (platform, entity_id, data, extracted_at)
                VALUES (?, ?, ?, ?)
            """, ("youtube", channel_id, json.dumps(historical_payload), now_iso))
            logger.info("Metric deltas detected. Recorded snapshot to historical_metrics for %s.", channel_id)
        else:
            logger.info("Deduplication active: 0 changes detected. Skipped historical_metrics append for %s.", channel_id)


# -----------------------------------------------------------------------------
# YouTube Data API v3 (Rule 3)
# -----------------------------------------------------------------------------
def fetch_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": BROWSER_HEADERS["User-Agent"]})
    try:
        with urllib.request.urlopen(req, context=DEFAULT_SSL_CONTEXT) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="ignore")
        try:
            error_json = json.loads(error_body)
            msg = error_json.get("error", {}).get("message", str(e))
        except Exception:
            msg = error_body or str(e)
        raise RuntimeError(f"YouTube API returned HTTP {e.code}: {msg}") from e
    except urllib.error.URLError as e:
        if isinstance(e.reason, ssl.SSLCertVerificationError) or "CERTIFICATE_VERIFY_FAILED" in str(e):
            unverified_ctx = ssl._create_unverified_context()
            with urllib.request.urlopen(req, context=unverified_ctx) as response:
                return json.loads(response.read().decode("utf-8"))
        raise RuntimeError(f"Network error communicating with YouTube API: {e.reason}") from e


def fetch_from_youtube_api(channel_id: str, api_key: str) -> dict:
    # Cascade Step 1: channels.list (1 unit)
    channels_url = (
        f"https://www.googleapis.com/youtube/v3/channels?"
        f"part=snippet,statistics,contentDetails&id={channel_id}&key={api_key}"
    )
    channel_data = fetch_json(channels_url)
    items = channel_data.get("items", [])
    if not items:
        raise ValueError(f"Channel ID '{channel_id}' does not exist or has no public visibility on YouTube.")

    item = items[0]
    artist_name = item.get("snippet", {}).get("title", "")
    stats = item.get("statistics", {})

    subscribers = int(stats.get("subscriberCount", 0))
    total_views = int(stats.get("viewCount", 0))
    video_count = int(stats.get("videoCount", 0))

    related_playlists = item.get("contentDetails", {}).get("relatedPlaylists", {})
    uploads_playlist_id = related_playlists.get("uploads")
    if not uploads_playlist_id:
        raise ValueError(f"Unable to locate 'Uploads' playlist for channel '{channel_id}'.")

    # Cascade Step 2: playlistItems.list (1 unit)
    playlist_url = (
        f"https://www.googleapis.com/youtube/v3/playlistItems?"
        f"part=contentDetails&playlistId={uploads_playlist_id}&maxResults=50&key={api_key}"
    )
    playlist_data = fetch_json(playlist_url)
    video_ids = [
        v["contentDetails"]["videoId"]
        for v in playlist_data.get("items", [])
        if "contentDetails" in v and "videoId" in v["contentDetails"]
    ]

    # Cascade Step 3: videos.list (1 unit)
    recent_50_views = 0
    recent_50_likes = 0
    recent_50_comments = 0

    if video_ids:
        video_ids_str = ",".join(video_ids)
        videos_url = (
            f"https://www.googleapis.com/youtube/v3/videos?"
            f"part=statistics&id={video_ids_str}&key={api_key}"
        )
        videos_data = fetch_json(videos_url)
        for vid in videos_data.get("items", []):
            vstats = vid.get("statistics", {})
            recent_50_views += int(vstats.get("viewCount", 0))
            recent_50_likes += int(vstats.get("likeCount", 0))
            recent_50_comments += int(vstats.get("commentCount", 0))

    return {
        "artist_id": channel_id,
        "artist_name": artist_name,
        "youtube_subscribers": subscribers,
        "youtube_total_views": total_views,
        "youtube_video_count": video_count,
        "youtube_recent_50_views": recent_50_views,
        "youtube_recent_50_likes": recent_50_likes,
        "youtube_recent_50_comments": recent_50_comments,
    }


# -----------------------------------------------------------------------------
# SocialBlade Extraction (Rule 3)
# -----------------------------------------------------------------------------
def fetch_socialblade_html(url: str) -> str:
    html = None
    # Attempt 1: curl_cffi for browser TLS fingerprint impersonation
    try:
        from curl_cffi import requests as cffi_requests
        response = cffi_requests.get(url, impersonate="chrome120", headers=BROWSER_HEADERS, timeout=15)
        if response.status_code == 200:
            html = response.text
        else:
            logger.warning("SocialBlade curl_cffi returned non-200 status code: %s", response.status_code)
    except ImportError:
        logger.debug("curl_cffi package not found; falling back to system curl execution.")
    except Exception as e:
        logger.warning("curl_cffi fetch failed: %s", e)

    # Attempt 2: Native system curl
    if not html:
        cmd = [
            "curl", "-sSL", "--compressed",
            "-A", BROWSER_HEADERS["User-Agent"],
            "-H", f"Accept: {BROWSER_HEADERS['Accept']}",
            "-H", f"Accept-Language: {BROWSER_HEADERS['Accept-Language']}",
            "-H", f"Sec-Ch-Ua: {BROWSER_HEADERS['Sec-Ch-Ua']}",
            "-H", f"Sec-Ch-Ua-Platform: {BROWSER_HEADERS['Sec-Ch-Ua-Platform']}",
            "-H", "Sec-Fetch-Dest: document",
            "-H", "Sec-Fetch-Mode: navigate",
            "-H", "Sec-Fetch-Site: none",
            "-H", "Sec-Fetch-User: ?1",
            "-H", "Upgrade-Insecure-Requests: 1",
            "--max-time", "15",
            url,
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            html = result.stdout
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"System curl command failed for SocialBlade URL '{url}': {e.stderr}") from e
        except FileNotFoundError as e:
            raise RuntimeError("Neither 'curl_cffi' nor system 'curl' binary is available in the environment.") from e

    if not html or "<html" not in html:
        raise RuntimeError(f"Empty or malformed HTML response received from SocialBlade for URL '{url}'.")

    if "Cloudflare" in html or "Just a moment..." in html:
        raise RuntimeError("SocialBlade request was blocked by a Cloudflare challenge.")

    if '<script id="__NEXT_DATA__"' not in html:
        raise RuntimeError("SocialBlade page received without required '__NEXT_DATA__' application payload.")

    return html


def parse_num(val_str):
    if not val_str:
        return None
    cleaned = str(val_str).replace(",", "").replace("+", "").replace("$", "").strip()
    multiplier = 1
    if cleaned.endswith(("K", "k")):
        multiplier = 1_000
        cleaned = cleaned[:-1]
    elif cleaned.endswith(("M", "m")):
        multiplier = 1_000_000
        cleaned = cleaned[:-1]
    elif cleaned.endswith(("B", "b")):
        multiplier = 1_000_000_000
        cleaned = cleaned[:-1]
    try:
        return int(float(cleaned) * multiplier)
    except ValueError:
        return None


def calculate_earnings(views):
    if views is None or views <= 0:
        return 0, 0
    low = int(round(views * 0.00025))
    high = int(round(views * 0.004))
    return low, high


def fetch_socialblade_metrics(channel_id: str) -> dict:
    url = f"https://socialblade.com/youtube/channel/{channel_id}"
    html = fetch_socialblade_html(url)

    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if not match:
        raise RuntimeError(f"Failed to isolate '__NEXT_DATA__' JSON payload from SocialBlade for channel '{channel_id}'.")

    try:
        next_data = json.loads(match.group(1))
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to decode SocialBlade '__NEXT_DATA__' JSON: {e}") from e

    queries = (
        next_data.get("props", {})
        .get("pageProps", {})
        .get("trpcState", {})
        .get("json", {})
        .get("queries", [])
    )

    metrics = {
        "sb_grade": None,
        "sb_created_at": None,
        "sb_rank": None,
        "sb_subscribers_rank": None,
        "sb_views_rank": None,
        "sb_country_rank": None,
        "sb_category_rank": None,
        "sb_subscribers_last_30_days": None,
        "sb_views_last_30_days": None,
        "sb_subscribers_last_14_days": None,
        "sb_views_last_14_days": None,
        "sb_daily_avg_views": None,
        "sb_monthly_earnings_min": None,
        "sb_monthly_earnings_max": None,
        "sb_yearly_earnings_min": None,
        "sb_yearly_earnings_max": None,
        "sb_history": [],
    }

    found_user_query = False
    for q in queries:
        q_key = q.get("queryKey", [])
        if not q_key:
            continue

        if q_key[0] == ["youtube", "user"]:
            u = q.get("state", {}).get("data")
            if not u:
                continue
            found_user_query = True
            metrics["sb_grade"] = u.get("grade")
            metrics["sb_created_at"] = u.get("createdAt")

            ranks = u.get("ranks") or {}
            metrics["sb_rank"] = ranks.get("sb")
            metrics["sb_subscribers_rank"] = ranks.get("subscribers")
            metrics["sb_views_rank"] = ranks.get("views")
            metrics["sb_country_rank"] = ranks.get("country")
            metrics["sb_category_rank"] = ranks.get("category")

        elif q_key[0] == ["youtube", "history"]:
            history_data = q.get("state", {}).get("data") or []
            clean_history = []

            for idx, row in enumerate(history_data):
                date_str = row.get("date", "").split("T")[0] if row.get("date") else None
                subs = row.get("subscribers")
                views = int(row.get("views", 0)) if row.get("views") is not None else None
                vids = row.get("videos")

                subs_gained = None
                views_gained = None
                is_active_sample = True

                if idx > 0:
                    prev = history_data[idx - 1]
                    prev_views = int(prev.get("views", 0)) if prev.get("views") is not None else None
                    prev_subs = prev.get("subscribers")
                    prev_vids = prev.get("videos")

                    if views == prev_views and subs == prev_subs and vids == prev_vids:
                        is_active_sample = False
                        views_gained = None
                        subs_gained = None
                    else:
                        if subs is not None and prev_subs is not None:
                            subs_gained = subs - prev_subs
                        if views is not None and prev_views is not None:
                            views_gained = views - prev_views

                clean_history.append({
                    "date": date_str,
                    "subscribers": subs,
                    "views": views,
                    "videos": vids,
                    "subscribers_gained": subs_gained,
                    "views_gained": views_gained,
                    "is_active_sample": is_active_sample,
                })

            metrics["sb_history"] = clean_history

            if len(clean_history) >= 2:
                h_first = clean_history[0]
                h_last = clean_history[-1]

                if h_last["views"] is not None and h_first["views"] is not None:
                    v_diff = h_last["views"] - h_first["views"]
                    metrics["sb_views_last_14_days"] = v_diff
                    days_span = max(1, len(clean_history) - 1)
                    metrics["sb_daily_avg_views"] = int(round(v_diff / days_span))

                if h_last["subscribers"] is not None and h_first["subscribers"] is not None:
                    metrics["sb_subscribers_last_14_days"] = h_last["subscribers"] - h_first["subscribers"]

    if not found_user_query:
        raise RuntimeError(f"User entity data for channel '{channel_id}' was not returned in SocialBlade payload.")

    m_30 = re.search(r"Last 30 Days[^\d+]*\+?([\d,]+)[KkMmBb]?[^\d+]*\+?([\d,]+)", html)
    if m_30:
        metrics["sb_subscribers_last_30_days"] = parse_num(m_30.group(1))
        metrics["sb_views_last_30_days"] = parse_num(m_30.group(2))

    if metrics["sb_views_last_30_days"] is None and metrics["sb_views_last_14_days"] is not None:
        metrics["sb_views_last_30_days"] = int(round((metrics["sb_views_last_14_days"] / 14.0) * 30.0))

    if metrics["sb_subscribers_last_30_days"] is None and metrics["sb_subscribers_last_14_days"] is not None:
        metrics["sb_subscribers_last_30_days"] = metrics["sb_subscribers_last_14_days"]

    ref_views_monthly = metrics["sb_views_last_30_days"] or metrics["sb_views_last_14_days"] or 0
    low_mo, high_mo = calculate_earnings(ref_views_monthly)
    metrics["sb_monthly_earnings_min"] = low_mo
    metrics["sb_monthly_earnings_max"] = high_mo
    metrics["sb_yearly_earnings_min"] = low_mo * 12
    metrics["sb_yearly_earnings_max"] = high_mo * 12

    return metrics


# -----------------------------------------------------------------------------
# CLI & Process Orchestration (Rules 2, 3, 4)
# -----------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Production YouTube & SocialBlade Metrics Extraction Module"
    )
    parser.add_argument(
        "channel_id",
        type=str,
        help="Target YouTube Channel ID (strictly required, e.g. UCjRqB-Y8NLetTH6OiNJWD1w)",
    )
    parser.add_argument(
        "-r", "--refresh", "--force", "-f",
        action="store_true",
        help="Bypass the 24-hour local SQLite cache and force a live extraction run.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    channel_id = args.channel_id.strip()

    if not channel_id:
        logger.error("Channel ID argument cannot be empty.")
        sys.exit(1)

    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key or not api_key.strip():
        logger.error("Missing required environment variable 'YOUTUBE_API_KEY'.")
        sys.exit(1)

    try:
        conn = init_db()
    except Exception as e:
        logger.error("Failed to initialize SQLite database at '%s': %s", DB_PATH, e, exc_info=True)
        sys.exit(1)

    data = None
    if not args.refresh:
        data = get_cached_data(conn, channel_id)

    if data is None:
        logger.info("Executing live data ingestion pipeline for channel '%s'...", channel_id)
        try:
            yt_metrics = fetch_from_youtube_api(channel_id, api_key)
            sb_metrics = fetch_socialblade_metrics(channel_id)
            data = {**yt_metrics, **sb_metrics}
            save_to_cache(conn, channel_id, data)
        except Exception as e:
            logger.error("Live extraction cascade failed for channel '%s': %s", channel_id, e, exc_info=True)
            conn.close()
            sys.exit(1)
    else:
        logger.info("Retrieved fresh cached data for channel '%s'.", channel_id)

    conn.close()

    logger.info("Scrape execution completed successfully.")
    logger.info("Normalized Output Payload:\n%s", json.dumps(data, indent=4))


if __name__ == "__main__":
    main()