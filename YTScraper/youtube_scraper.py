#!/usr/bin/env python3
import os
import sys
import json
import sqlite3
import time
import re
import argparse
import subprocess
import urllib.request
import urllib.error
import ssl

try:
    import certifi
    DEFAULT_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except Exception:
    try:
        DEFAULT_SSL_CONTEXT = ssl.create_default_context()
    except Exception:
        DEFAULT_SSL_CONTEXT = ssl._create_unverified_context()

DB_PATH = 'socials_cache.db'
TTL_SECONDS = 86400

BROWSER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Sec-Ch-Ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"macOS"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1'
}

def fetch_json(url):
    req = urllib.request.Request(url, headers={'User-Agent': BROWSER_HEADERS['User-Agent']})
    try:
        with urllib.request.urlopen(req, context=DEFAULT_SSL_CONTEXT) as response:
            return json.loads(response.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8', errors='ignore')
        try:
            error_json = json.loads(error_body)
            msg = error_json.get('error', {}).get('message', str(e))
        except Exception:
            msg = error_body or str(e)
        raise RuntimeError(f"YouTube API HTTP {e.code} error: {msg}")
    except urllib.error.URLError as e:
        if isinstance(e.reason, ssl.SSLCertVerificationError) or "CERTIFICATE_VERIFY_FAILED" in str(e):
            unverified_ctx = ssl._create_unverified_context()
            with urllib.request.urlopen(req, context=unverified_ctx) as response:
                return json.loads(response.read().decode('utf-8'))
        raise RuntimeError(f"Network error fetching YouTube API: {e.reason}")

def fetch_socialblade_html(url):
    # Strategy 1: curl_cffi (matches browser TLS fingerprint to bypass Cloudflare)
    try:
        from curl_cffi import requests as cffi_requests
        response = cffi_requests.get(url, impersonate="chrome120", headers=BROWSER_HEADERS, timeout=15)
        if response.status_code == 200:
            return response.text
        else:
            print(f"[Warning] SocialBlade returned HTTP {response.status_code} via curl_cffi", file=sys.stderr)
    except ImportError:
        pass
    except Exception as e:
        print(f"[Warning] curl_cffi attempt failed: {e}", file=sys.stderr)

    # Strategy 2: System curl fallback
    cmd = [
        'curl', '-sSL', '--compressed',
        '-A', BROWSER_HEADERS['User-Agent'],
        '-H', f"Accept: {BROWSER_HEADERS['Accept']}",
        '-H', f"Accept-Language: {BROWSER_HEADERS['Accept-Language']}",
        '-H', f"Sec-Ch-Ua: {BROWSER_HEADERS['Sec-Ch-Ua']}",
        '-H', f"Sec-Ch-Ua-Platform: {BROWSER_HEADERS['Sec-Ch-Ua-Platform']}",
        '-H', 'Sec-Fetch-Dest: document',
        '-H', 'Sec-Fetch-Mode: navigate',
        '-H', 'Sec-Fetch-Site: none',
        '-H', 'Sec-Fetch-User: ?1',
        '-H', 'Upgrade-Insecure-Requests: 1',
        '--max-time', '15',
        url
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        html = result.stdout
        if '<script id="__NEXT_DATA__"' in html:
            return html
        elif 'Cloudflare' in html or 'Just a moment...' in html:
            print("[Warning] SocialBlade request was intercepted by Cloudflare challenge. "
                  "Install curl_cffi (`pip install curl_cffi`) for automated TLS bypass.", file=sys.stderr)
            return None
        return html
    except Exception as e:
        print(f"[Warning] System curl failed for SocialBlade: {e}", file=sys.stderr)
        return None

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute('PRAGMA journal_mode = WAL;')
    conn.execute('PRAGMA synchronous = NORMAL;')
    
    # 1. Existing cache table (for 24-hour TTL checks)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS metrics_cache (
            channel_id TEXT PRIMARY KEY,
            data JSON,
            updated_at INTEGER
        )
    ''')
    
    # 2. Permanent append-only multi-platform time-series table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS historical_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            platform TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            data JSON NOT NULL,
            extracted_at INTEGER NOT NULL
        )
    ''')
    
    # Compound index for fast time-series lookups across platforms & entities
    conn.execute('''
        CREATE INDEX IF NOT EXISTS idx_historical_platform_entity_extracted
        ON historical_metrics (platform, entity_id, extracted_at DESC)
    ''')
    
    conn.commit()
    return conn

def get_cached_data(conn, channel_id):
    cursor = conn.cursor()
    cursor.execute('SELECT data, updated_at FROM metrics_cache WHERE channel_id = ?', (channel_id,))
    row = cursor.fetchone()
    if row:
        data, updated_at = row
        if time.time() - updated_at < TTL_SECONDS:
            return json.loads(data)
    return None

def save_to_cache(conn, channel_id, data):
    cursor = conn.cursor()
    now_ts = int(time.time())
    data_json = json.dumps(data)

    # Execute both operations atomically in a single transaction
    with conn:
        # Action A: Upsert into 24-hr TTL metrics_cache table
        cursor.execute('''
            INSERT INTO metrics_cache (channel_id, data, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET
                data = excluded.data,
                updated_at = excluded.updated_at
        ''', (channel_id, data_json, now_ts))

        # Action B: Append new snapshot into historical_metrics table
        cursor.execute('''
            INSERT INTO historical_metrics (platform, entity_id, data, extracted_at)
            VALUES (?, ?, ?, ?)
        ''', ('youtube', channel_id, data_json, now_ts))

def parse_num(val_str):
    if not val_str:
        return None
    cleaned = str(val_str).replace(',', '').replace('+', '').replace('$', '').strip()
    multiplier = 1
    if cleaned.endswith(('K', 'k')):
        multiplier = 1_000
        cleaned = cleaned[:-1]
    elif cleaned.endswith(('M', 'm')):
        multiplier = 1_000_000
        cleaned = cleaned[:-1]
    elif cleaned.endswith(('B', 'b')):
        multiplier = 1_000_000_000
        cleaned = cleaned[:-1]
    try:
        return int(float(cleaned) * multiplier)
    except ValueError:
        return None

def calculate_earnings(views):
    """
    Calculates SocialBlade standard estimated earnings based on
    $0.25 (low) to $4.00 (high) CPM per 1,000 views.
    """
    if views is None or views <= 0:
        return 0, 0
    low = int(round(views * 0.00025))
    high = int(round(views * 0.004))
    return low, high

def fetch_socialblade_metrics(channel_id):
    url = f"https://socialblade.com/youtube/channel/{channel_id}"
    html = fetch_socialblade_html(url)
    
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
        "sb_history": []
    }
    
    if not html:
        return metrics

    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if not match:
        return metrics

    try:
        next_data = json.loads(match.group(1))
        queries = next_data.get('props', {}).get('pageProps', {}).get('trpcState', {}).get('json', {}).get('queries', [])
        
        for q in queries:
            q_key = q.get('queryKey', [])
            if not q_key:
                continue

            # User Profile: [["youtube", "user"], ...]
            if q_key[0] == ["youtube", "user"]:
                u = q.get('state', {}).get('data') or {}
                metrics["sb_grade"] = u.get("grade")
                metrics["sb_created_at"] = u.get("createdAt")
                
                ranks = u.get("ranks") or {}
                metrics["sb_rank"] = ranks.get("sb")
                metrics["sb_subscribers_rank"] = ranks.get("subscribers")
                metrics["sb_views_rank"] = ranks.get("views")
                metrics["sb_country_rank"] = ranks.get("country")
                metrics["sb_category_rank"] = ranks.get("category")

            # History Data Table: [["youtube", "history"], ...]
            elif q_key[0] == ["youtube", "history"]:
                history_data = q.get('state', {}).get('data') or []
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

                        # Carryover / unpolled day check: identical views, subs, and vids
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
                        "is_active_sample": is_active_sample
                    })

                metrics["sb_history"] = clean_history

                # Compute exact 14-day metrics from the first and last snapshot in the window
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

    except Exception as e:
        print(f"[Warning] Failed parsing SocialBlade JSON data: {e}", file=sys.stderr)

    # Search for pre-rendered 30-day stats in HTML
    m_30 = re.search(r'Last 30 Days[^\d+]*\+?([\d,]+)[KkMmBb]?[^\d+]*\+?([\d,]+)', html)
    if m_30:
        metrics["sb_subscribers_last_30_days"] = parse_num(m_30.group(1))
        metrics["sb_views_last_30_days"] = parse_num(m_30.group(2))

    # Normalized 30-day fallback from observed 14-day sample
    if metrics["sb_views_last_30_days"] is None and metrics["sb_views_last_14_days"] is not None:
        metrics["sb_views_last_30_days"] = int(round((metrics["sb_views_last_14_days"] / 14.0) * 30.0))

    if metrics["sb_subscribers_last_30_days"] is None and metrics["sb_subscribers_last_14_days"] is not None:
        metrics["sb_subscribers_last_30_days"] = metrics["sb_subscribers_last_14_days"]

    # Earnings calculations based on 30-day view reference
    ref_views_monthly = metrics["sb_views_last_30_days"] or metrics["sb_views_last_14_days"] or 0
    low_mo, high_mo = calculate_earnings(ref_views_monthly)
    metrics["sb_monthly_earnings_min"] = low_mo
    metrics["sb_monthly_earnings_max"] = high_mo
    metrics["sb_yearly_earnings_min"] = low_mo * 12
    metrics["sb_yearly_earnings_max"] = high_mo * 12

    return metrics

def fetch_from_youtube_api(channel_id, api_key):
    # Step 1: channels.list
    channels_url = (
        f"https://www.googleapis.com/youtube/v3/channels?"
        f"part=snippet,statistics,contentDetails&id={channel_id}&key={api_key}"
    )
    channel_data = fetch_json(channels_url)
    if not channel_data.get('items'):
        raise ValueError(f"Channel ID '{channel_id}' does not exist or has no public access on YouTube.")

    item = channel_data['items'][0]
    artist_name = item['snippet'].get('title', '')
    stats = item.get('statistics', {})
    
    subscribers = int(stats.get('subscriberCount', 0))
    total_views = int(stats.get('viewCount', 0))
    video_count = int(stats.get('videoCount', 0))
    
    related_playlists = item.get('contentDetails', {}).get('relatedPlaylists', {})
    uploads_playlist_id = related_playlists.get('uploads')

    if not uploads_playlist_id:
        raise ValueError(f"Could not locate 'Uploads' playlist for channel '{channel_id}'.")

    # Step 2: playlistItems.list
    playlist_url = (
        f"https://www.googleapis.com/youtube/v3/playlistItems?"
        f"part=contentDetails&playlistId={uploads_playlist_id}&maxResults=50&key={api_key}"
    )
    playlist_data = fetch_json(playlist_url)
    video_ids = [v['contentDetails']['videoId'] for v in playlist_data.get('items', []) if 'contentDetails' in v]

    # Step 3: videos.list
    recent_50_views = 0
    recent_50_likes = 0
    recent_50_comments = 0

    if video_ids:
        video_ids_str = ','.join(video_ids)
        videos_url = (
            f"https://www.googleapis.com/youtube/v3/videos?"
            f"part=statistics&id={video_ids_str}&key={api_key}"
        )
        videos_data = fetch_json(videos_url)
        for vid in videos_data.get('items', []):
            vstats = vid.get('statistics', {})
            recent_50_views += int(vstats.get('viewCount', 0))
            recent_50_likes += int(vstats.get('likeCount', 0))
            recent_50_comments += int(vstats.get('commentCount', 0))

    return {
        "artist_id": channel_id,
        "artist_name": artist_name,
        "youtube_subscribers": subscribers,
        "youtube_total_views": total_views,
        "youtube_video_count": video_count,
        "youtube_recent_50_views": recent_50_views,
        "youtube_recent_50_likes": recent_50_likes,
        "youtube_recent_50_comments": recent_50_comments
    }

def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract quota-optimized YouTube API & SocialBlade engagement metrics."
    )
    parser.add_argument(
        'channel_pos',
        nargs='?',
        default=None,
        help="Target YouTube Channel ID"
    )
    parser.add_argument(
        '-c', '--channel-id', '--channel',
        dest='channel_opt',
        default=None,
        help="Target YouTube Channel ID"
    )
    parser.add_argument(
        '-r', '--refresh', '--force', '-f',
        action='store_true',
        help="Bypass 24-hour cache and perform fresh lookups."
    )
    return parser.parse_args()

def main():
    args = parse_args()
    channel_id = args.channel_opt or args.channel_pos

    if not channel_id:
        print("ERROR: Missing target YouTube Channel ID.", file=sys.stderr)
        print("Usage: python3 youtube_scraper.py <CHANNEL_ID> [--refresh]", file=sys.stderr)
        sys.exit(1)

    api_key = os.environ.get('YOUTUBE_API_KEY')
    if not api_key or not api_key.strip():
        print("ERROR: Environment variable YOUTUBE_API_KEY is not set or is empty.", file=sys.stderr)
        print("Set it using: export YOUTUBE_API_KEY=\"your_key_here\" or prefix the command.", file=sys.stderr)
        sys.exit(1)

    conn = init_db()
    data = None if args.refresh else get_cached_data(conn, channel_id)

    if data is None:
        try:
            yt_metrics = fetch_from_youtube_api(channel_id, api_key)
        except Exception as e:
            print(f"ERROR during YouTube API execution: {e}", file=sys.stderr)
            conn.close()
            sys.exit(1)

        sb_metrics = fetch_socialblade_metrics(channel_id)
        
        data = {**yt_metrics, **sb_metrics}
        save_to_cache(conn, channel_id, data)
    else:
        print(f"Using cached data for {channel_id} (use --refresh to invalidate cache)...", file=sys.stderr)

    print("\n--- JSON OUTPUT ---")
    print(json.dumps(data, indent=4))
    conn.close()

if __name__ == '__main__':
    main()