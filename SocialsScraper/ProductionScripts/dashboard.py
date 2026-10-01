import streamlit as st
import sqlite3
import pandas as pd
import os
from datetime import datetime

DB_PATH = "data/socials_cache.db"

# Ensure DB dir exists for initial boot
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def init_db():
    """Ensures core tables exist before the dashboard tries to read/write them."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode = WAL;")
        # Ensure artist table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS artist_identities (
                songstats_id TEXT PRIMARY KEY,
                artist_name TEXT,
                spotify_id TEXT,
                youtube_id TEXT,
                instagram_handle TEXT,
                tiktok_handle TEXT,
                soundcloud_handle TEXT,
                apple_music_id TEXT,
                beatport_id TEXT,
                deezer_id TEXT,
                amazon_id TEXT,
                tidal_id TEXT,
                shazam_id TEXT,
                updated_at TIMESTAMP
            )
        """)
        # Ensure job queue exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS job_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                artist_id TEXT,
                platform TEXT,
                job_type TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            )
        """)
        # Ensure health table exists
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

# Initialize the database tables on boot
init_db()

def get_db_connection():
    return sqlite3.connect(DB_PATH)

st.set_page_config(page_title="Music Analytics HQ", page_icon="🎵", layout="wide")
st.title("🎵 Music Analytics Control Center")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🏠 System Health", 
    "👥 Manage Artists", 
    "▶️ Manual Scrapes", 
    "🗄️ Database Browser", 
    "💾 Backup"
])

# ==========================================
# TAB 1: SYSTEM & API HEALTH
# ==========================================
with tab1:
    st.header("Pipeline Health & Quotas")
    
    conn = get_db_connection()
    try:
        # Script Health Table
        health_df = pd.read_sql_query("SELECT * FROM script_health ORDER BY last_run DESC", conn)
        
        # Calculate approximate API usage today
        today_date = pd.Timestamp.utcnow().strftime('%Y-%m-%d')
        cur = conn.execute("SELECT platform, COUNT(*) FROM job_queue WHERE status='completed' AND date(completed_at) = date('now') GROUP BY platform")
        jobs_today = dict(cur.fetchall())
        
        yt_cost = jobs_today.get('youtube', 0) * 3
        apify_cost = jobs_today.get('tiktok', 0) * 0.002
        
        col1, col2, col3 = st.columns(3)
        col1.metric("YouTube API Quota Used Today", f"{yt_cost} / 10,000", help="Each run costs 3 units.")
        col2.metric("Apify Spend Today", f"${apify_cost:.4f}", help="Each run costs ~$0.002.")
        col3.metric("Scrapes Completed Today", sum(jobs_today.values()))

        st.subheader("Latest Script Executions")
        
        # Stylize Status Column
        def color_status(val):
            color = 'green' if val == 'completed' else 'red' if val == 'failed' else 'orange'
            return f'color: {color}'
            
        if not health_df.empty:
            st.dataframe(health_df.style.map(color_status, subset=['status']), use_container_width=True, hide_index=True)
        else:
            st.info("No scripts have run yet.")
    except Exception as e:
        st.warning(f"Database initializing... {e}")
    finally:
        conn.close()

# ==========================================
# TAB 2: MANAGE ARTISTS
# ==========================================
with tab2:
    st.header("Artist Identity Graph")
    
    with st.form("add_artist_form"):
        st.write("Add an artist by their Songstats URL. The pipeline will automatically figure out the rest.")
        url = st.text_input("Songstats URL", placeholder="https://songstats.com/artist/y4ku2hlc/k-law")
        tiktok_override = st.text_input("TikTok Handle (Optional Override if missing from Songstats)", placeholder="e.g., k.law__")
        
        if st.form_submit_button("Onboard Artist"):
            if url:
                songstats_id = url.split("artist/")[1].split("/")[0] if "artist/" in url else url
                conn = get_db_connection()
                try:
                    # Insert placeholder, then trigger queue to fetch the rest
                    conn.execute("INSERT OR IGNORE INTO artist_identities (songstats_id, updated_at) VALUES (?, CURRENT_TIMESTAMP)", (songstats_id,))
                    if tiktok_override:
                        conn.execute("UPDATE artist_identities SET tiktok_handle = ? WHERE songstats_id = ?", (tiktok_override, songstats_id))
                    
                    # Queue the Songstats Scraper to finish the job immediately
                    conn.execute("INSERT INTO job_queue (artist_id, platform, job_type) VALUES (?, 'songstats', 'manual')", (songstats_id,))
                    conn.commit()
                    st.success("Artist onboarded and identity scrape queued! Check the 'System Health' tab.")
                except Exception as e:
                    st.error(f"Error: {e}")
                finally:
                    conn.close()
            else:
                st.warning("Please provide a URL.")
                
    conn = get_db_connection()
    try:
        roster_df = pd.read_sql_query("SELECT songstats_id, artist_name, spotify_id, youtube_id, instagram_handle, tiktok_handle FROM artist_identities", conn)
        st.dataframe(roster_df, use_container_width=True, hide_index=True)
    except Exception:
        pass
    conn.close()

# ==========================================
# TAB 3: MANUAL SCRAPES
# ==========================================
with tab3:
    st.header("Trigger Manual Scrapes")
    st.write("Jobs submitted here are prioritized by the Orchestrator and bypass the standard anti-bot delay.")
    
    conn = get_db_connection()
    try:
        cur = conn.execute("SELECT songstats_id, artist_name FROM artist_identities WHERE artist_name IS NOT NULL")
        artist_map = {row[1]: row[0] for row in cur.fetchall()}
        
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            sel_artist = st.selectbox("Select Artist", list(artist_map.keys()))
        with col2:
            platforms = ["youtube", "spotify", "instagram", "tiktok", "songstats", "songstats_spotify"]
            sel_plat = st.selectbox("Select Platform", platforms)
        with col3:
            st.write("") # Spacing
            st.write("")
            if st.button("▶️ Queue Job"):
                if sel_artist:
                    s_id = artist_map[sel_artist]
                    conn.execute("INSERT INTO job_queue (artist_id, platform, job_type) VALUES (?, ?, 'manual')", (s_id, sel_plat))
                    conn.commit()
                    st.success(f"Queued {sel_plat} for {sel_artist}!")

        st.divider()
        st.subheader("Current Job Queue")
        queue_df = pd.read_sql_query("SELECT id, artist_id, platform, job_type, status, created_at, completed_at FROM job_queue ORDER BY created_at DESC LIMIT 20", conn)
        st.dataframe(queue_df, use_container_width=True, hide_index=True)
        
    except Exception as e:
        st.info("Please onboard an artist first.")
    finally:
        conn.close()

# ==========================================
# TAB 4: DATABASE BROWSER
# ==========================================
with tab4:
    st.header("Raw Database Explorer")
    conn = get_db_connection()
    try:
        # Get all table names
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [r[0] for r in cur.fetchall() if "sqlite" not in r[0]]
        
        if tables:
            sel_table = st.selectbox("Select Table to View", tables)
            df = pd.read_sql_query(f"SELECT * FROM {sel_table} LIMIT 1000", conn)
            st.dataframe(df, use_container_width=True)
        else:
            st.info("Database is empty.")
    except Exception as e:
        st.error(f"Error accessing database: {e}")
    finally:
        conn.close()

# ==========================================
# TAB 5: BACKUPS
# ==========================================
with tab5:
    st.header("Download SQLite Database")
    st.write("Export a complete, frozen snapshot of the `socials_cache.db` file for local backup or analysis in third-party tools.")
    
    if os.path.exists(DB_PATH):
        # Read file as bytes
        with open(DB_PATH, "rb") as fp:
            st.download_button(
                label="⬇️ Download socials_cache.db",
                data=fp,
                file_name=f"socials_cache_{datetime.now().strftime('%Y%m%d')}.db",
                mime="application/octet-stream"
            )
    else:
        st.error("Database file not found.")