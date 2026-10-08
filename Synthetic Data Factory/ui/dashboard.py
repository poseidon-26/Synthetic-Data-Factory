import streamlit as st
import sqlite3
import pandas as pd
import time
import os
import glob
import psutil
import subprocess
import datetime

# --- Configuration & Theme ---
st.set_page_config(
    page_title="Agile Day-Router | Telemetry Console", 
    layout="wide", 
    page_icon="⚡",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
    /* Institutional Dark Theme */
    .stApp { background-color: #0b0f19; color: #e2e8f0; }
    h1, h2, h3 { font-family: 'Inter', sans-serif; color: #ffffff; }
    
    /* Layout Primitives */
    .panel-container {
        background-color: #111827;
        border-radius: 8px;
        padding: 20px;
        border: 1px solid #1f2937;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.5);
        margin-bottom: 20px;
        height: 100%;
    }
    .panel-header {
        font-size: 1.1rem;
        font-weight: 700;
        color: #94a3b8;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        border-bottom: 1px solid #334155;
        padding-bottom: 10px;
        margin-bottom: 20px;
    }
    
    /* Status Badge */
    .status-badge {
        display: inline-block;
        padding: 5px 15px;
        border-radius: 20px;
        font-weight: bold;
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    .status-GENERATING { background-color: #047857; color: #d1fae5; }
    .status-STREAMING { background-color: #2563eb; color: #dbeafe; }
    .status-IDLE { background-color: #475569; color: #f1f5f9; }
    .status-COMPLETED { background-color: #7e22ce; color: #f3e8ff; }
    
    /* Fidelity Checkmark */
    .fidelity-pass { color: #10b981; font-weight: bold; font-size: 1.2rem; }
    .fidelity-fail { color: #ef4444; font-weight: bold; font-size: 1.2rem; }
</style>
""", unsafe_allow_html=True)

# --- Constants & Utilities ---
BUFFER_DIR = r"C:\Data_Buffer"
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'database', 'pipeline_state.db'))
TARGET_LIMIT_BYTES = 25 * 1024 * 1024 * 1024  # 25GB

TRACK_MAP = {
    "Monday": "Track 1: Multi-Turn Agentic Tool-Calling",
    "Tuesday": "Track 2: Code Synthesis & Debugging",
    "Wednesday": "Track 3: Cyber-Sec Zero-Day Defense",
    "Thursday": "Track 4: DeFi Quant & MEV",
    "Friday": "Track 5: Clinical Diagnostics",
    "Saturday": "Track 6: Safety & Red-Teaming Pool",
    "Sunday": "Track 7: Math PRM Algorithmic Pool"
}

def get_gpu_metrics():
    try:
        output = subprocess.check_output(
            ['nvidia-smi', '--query-gpu=utilization.gpu,memory.used', '--format=csv,noheader,nounits'],
            text=True, timeout=2
        )
        util, mem = output.strip().split(', ')
        return f"{util}%", f"{mem} MB"
    except Exception:
        return "N/A", "N/A"

def fetch_pipeline_data():
    try:
        if not os.path.exists(DB_PATH):
            return pd.DataFrame()
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT * FROM data_pipeline", conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

# --- State Initialization ---
if 'last_time' not in st.session_state:
    st.session_state.last_time = time.time()
    st.session_state.last_bytes = 0.0

# --- Core Logic Loop ---
zone_ph = st.empty()

while True:
    current_time = time.time()
    df = fetch_pipeline_data()
    
    # Context Logic
    current_day = datetime.datetime.now().strftime("%A")
    track_name = TRACK_MAP.get(current_day, "Unknown Track")
    
    # Buffer Metrics
    buffer_parquet_files = 0
    buffer_tmp_files = 0
    bytes_on_disk = 0
    if os.path.exists(BUFFER_DIR):
        for f in os.listdir(BUFFER_DIR):
            fp = os.path.join(BUFFER_DIR, f)
            if os.path.isfile(fp):
                bytes_on_disk += os.path.getsize(fp)
                if f.endswith('.parquet'):
                    buffer_parquet_files += 1
                elif f.endswith('.tmp'):
                    buffer_tmp_files += 1

    # Database Metrics
    total_rows = int(df['row_count'].sum()) if not df.empty and 'row_count' in df else 0
    completed_chunks = len(df[df['upload_status'] == 'COMPLETED']) if not df.empty and 'upload_status' in df else 0
    pending_df = df[df['upload_status'].isin(['PENDING', 'UPLOADING'])] if not df.empty and 'upload_status' in df else pd.DataFrame()
    
    # Status Determination
    estimated_completed_bytes = completed_chunks * 5 * 1024 * 1024 * 1024  # Assuming 5GB target per finalized chunk
    total_generated_bytes = estimated_completed_bytes + bytes_on_disk
    
    if total_generated_bytes >= TARGET_LIMIT_BYTES:
        system_status = "COMPLETED"
    elif not pending_df.empty or buffer_parquet_files > 0:
        system_status = "STREAMING"
    elif buffer_tmp_files > 0:
        system_status = "GENERATING"
    else:
        system_status = "IDLE"
        
    # Throughput Math (MB/s)
    time_delta = current_time - st.session_state.last_time
    bytes_delta = total_generated_bytes - st.session_state.last_bytes
    
    current_mb_ps = 0.0
    if time_delta > 0 and bytes_delta > 0:
        current_mb_ps = (bytes_delta / (1024 * 1024)) / time_delta
        
    # ETA Math
    bytes_remaining = max(0, TARGET_LIMIT_BYTES - total_generated_bytes)
    eta_str = "Calculating..."
    if current_mb_ps > 0:
        eta_seconds = (bytes_remaining / (1024 * 1024)) / current_mb_ps
        eta_str = str(datetime.timedelta(seconds=int(eta_seconds)))
    elif system_status == "COMPLETED":
        eta_str = "00:00:00"
    elif system_status == "IDLE":
        eta_str = "System Idle"

    # Save state
    st.session_state.last_time = current_time
    st.session_state.last_bytes = total_generated_bytes

    # UI Rendering
    with zone_ph.container():
        # --- 1. Core Execution Context Bar ---
        st.markdown(f"""
        <div class="panel-container" style="display: flex; justify-content: space-between; align-items: center; padding: 15px 30px;">
            <div>
                <div style="font-size: 0.9rem; color: #94a3b8; font-weight: bold; text-transform: uppercase;">Active Runtime</div>
                <div style="font-size: 1.5rem; color: #f8fafc; font-weight: 800;">{current_day} | {track_name}</div>
            </div>
            <div>
                <span class="status-badge status-{system_status}">{system_status}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Grid Layout
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            # --- 2. High-Velocity Production Metrics ---
            st.markdown('<div class="panel-container"><div class="panel-header">Production Metrics</div>', unsafe_allow_html=True)
            st.metric(label="Live Accumulated Rows", value=f"{total_rows:,}")
            
            c1, c2 = st.columns(2)
            c1.metric(label="Finalized Chunks (Local)", value=f"{buffer_parquet_files}")
            c2.metric(label="Anchored to HF (Remote)", value=f"{completed_chunks}")
            st.markdown('</div>', unsafe_allow_html=True)
            
        with col2:
            # --- 3. Live System Telemetry Engine ---
            st.markdown('<div class="panel-container"><div class="panel-header">System Telemetry</div>', unsafe_allow_html=True)
            cpu_util = psutil.cpu_percent(interval=None)
            gpu_util, gpu_mem = get_gpu_metrics()
            
            c1, c2 = st.columns(2)
            c1.metric(label="Global CPU Utilization", value=f"{cpu_util}%")
            c2.metric(label="GPU Core Utilization", value=gpu_util)
            st.metric(label="GPU VRAM Footprint", value=gpu_mem)
            st.markdown('</div>', unsafe_allow_html=True)
            
        with col3:
            # --- 5. Rigorous Data Quality Verification ---
            st.markdown('<div class="panel-container"><div class="panel-header">Data Quality Audit</div>', unsafe_allow_html=True)
            
            latest_entropy = 0.00
            jsd_value = 0.00
            if not df.empty and 'entropy_score' in df:
                valid_entropy = df['entropy_score'].dropna()
                if not valid_entropy.empty:
                    latest_entropy = float(valid_entropy.iloc[-1])
                if len(valid_entropy) > 1:
                    jsd_value = float(valid_entropy.tail(10).std())
                    if pd.isna(jsd_value):
                        jsd_value = 0.00
                        
            c1, c2 = st.columns(2)
            c1.metric(label="Live Shannon Entropy", value=f"{latest_entropy:.4f}")
            c2.metric(label="Live JSD (10-Chunk StdDev)", value=f"{jsd_value:.4f}")
            
            is_valid = latest_entropy >= 0.90 and jsd_value <= 0.05
            fidelity_class = "fidelity-pass" if is_valid else "fidelity-fail"
            fidelity_text = "PASS (AI-Grade Pool)" if is_valid else "FAIL (Calibrating...)"
            if df.empty:
                fidelity_text = "AWAITING DATA"
                fidelity_class = "fidelity-fail"
                
            st.markdown(f"**Structural Fidelity:** <span class='{fidelity_class}'>{fidelity_text}</span>", unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
            
        # --- 4. Dual-Phase Progress Bar & Countdown Circuit ---
        st.markdown('<div class="panel-container"><div class="panel-header">Progress & Ingestion Stream</div>', unsafe_allow_html=True)
        
        pct_complete = min(1.0, total_generated_bytes / TARGET_LIMIT_BYTES)
        st.progress(pct_complete)
        
        c1, c2, c3 = st.columns(3)
        c1.metric(label="Total Session Volume", value=f"{(total_generated_bytes / (1024**3)):.2f} / 25 GB")
        c2.metric(label="Live Throughput", value=f"{current_mb_ps:.2f} MB/s")
        c3.metric(label="ETA to Graceful Shutdown", value=eta_str)
        
        st.markdown("**Live Ingestion Stream (Pending Uploads)**")
        if not pending_df.empty:
            st.dataframe(
                pending_df[['file_name', 'data_category', 'row_count', 'upload_status']],
                use_container_width=True,
                hide_index=True,
                height=150
            )
        else:
            st.info("No active streams. Buffer is empty.", icon="✅")
            
        st.markdown('</div>', unsafe_allow_html=True)

    time.sleep(1.0)
