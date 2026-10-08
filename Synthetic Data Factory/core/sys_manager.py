import os
import time
import ctypes
import sqlite3
import subprocess
import threading
import logging
import re
from datetime import datetime, date

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [SRE Manager] - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'database', 'pipeline_state.db')
BUFFER_DIR = 'C:\\Data_Buffer'
REPORTS_DIR = os.path.join(BASE_DIR, 'reports')
os.makedirs(REPORTS_DIR, exist_ok=True)

# OS Power Lock Flags
ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001
ES_DISPLAY_REQUIRED = 0x00000002

def get_db_connection():
    """Returns a SQLite connection."""
    conn = sqlite3.connect(DB_PATH, timeout=20.0)
    return conn

def enforce_os_power_lock():
    """Overrides Windows sleep/hibernation routines and sets High Performance mode."""
    try:
        # Prevent Sleep
        ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED)
        logger.info("Windows API sleep/hibernation protocols OVERRIDDEN. System is locked awake.")
        
        # Set High Performance Power Plan (UUID standard across Windows)
        subprocess.run(["powercfg", "-setactive", "8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c"], check=False)
        logger.info("High-Performance power plan enforced.")
    except Exception as e:
        logger.error(f"Failed to enforce OS power lock: {e}")

def get_gpu_temperature():
    """Queries NVIDIA-SMI for current GPU temperature with fallback regex parsing."""
    try:
        # Attempt direct subprocess call (assuming nvidia-smi is in PATH)
        # Using full path as a safe fallback just in case
        nvsmi_path = 'nvidia-smi'
        if os.path.exists(r"C:\Program Files\NVIDIA Corporation\NVSMI\nvidia-smi.exe"):
            nvsmi_path = r"C:\Program Files\NVIDIA Corporation\NVSMI\nvidia-smi.exe"
            
        result = subprocess.check_output(
            [nvsmi_path, '--query-gpu=temperature.gpu', '--format=csv,noheader'], 
            encoding='utf-8'
        )
        # Safe fallback regex parsing in case of unexpected string formats
        match = re.search(r'\d+', result)
        if match:
            return int(match.group())
        return -1
    except Exception as e:
        return -1

def diagnostic_burn_test_loop():
    """Continuously monitors thermals and purges stagnant buffer files."""
    logger.info("Starting Diagnostic Telemetry Guard & Stagnation Purger...")
    while True:
        try:
            # 1. Thermal Throttle Logic
            gpu_temp = get_gpu_temperature()
            if gpu_temp != -1:
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT thermal_throttle FROM system_config WHERE id=1")
                row = cursor.fetchone()
                if row:
                    current_throttle = row[0]
                    
                    if gpu_temp > 85 and current_throttle == 0:
                        logger.warning(f"CRITICAL GPU TEMP: {gpu_temp}°C! Triggering thermal throttle.")
                        cursor.execute("UPDATE system_config SET thermal_throttle=1 WHERE id=1")
                        conn.commit()
                    elif gpu_temp < 75 and current_throttle == 1:
                        logger.info(f"GPU Temp normalized: {gpu_temp}°C. Lifting thermal throttle.")
                        cursor.execute("UPDATE system_config SET thermal_throttle=0 WHERE id=1")
                        conn.commit()
                conn.close()

            # 2. Buffer Purger (Delete parquets > 90 mins old)
            if os.path.exists(BUFFER_DIR):
                current_time = time.time()
                for filename in os.listdir(BUFFER_DIR):
                    filepath = os.path.join(BUFFER_DIR, filename)
                    if os.path.isfile(filepath):
                        file_age_seconds = current_time - os.path.getmtime(filepath)
                        if file_age_seconds > 5400: # 90 minutes
                            try:
                                # OS-level lock check: rename to itself
                                os.rename(filepath, filepath)
                                os.remove(filepath)
                                logger.warning(f"PURGED stagnant file from buffer: {filename} (Age: {file_age_seconds:.0f}s)")
                            except PermissionError:
                                logger.info(f"File {filename} is locked by active generator. Skipping purge.")
                            except Exception as e:
                                logger.error(f"Failed to purge {filename}: {e}")

        except Exception as e:
            logger.error(f"Error in diagnostic loop: {e}")
            
        time.sleep(30)

def daily_rotation_loop():
    """Handles midnight rotations (circuit breaker reset)."""
    while True:
        now = datetime.now()
        if now.hour == 0 and now.minute == 0:
            today_str = str(date.today())
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT last_reset_date FROM system_config WHERE id=1")
                row = cursor.fetchone()
                if row:
                    last_reset = row[0]
                    
                    if last_reset != today_str:
                        cursor.execute("""
                            UPDATE system_config 
                            SET active_niche='AgenticToolCalling', daily_upload_bytes=0, circuit_breaker=0, last_reset_date=? 
                            WHERE id=1
                        """, (today_str,))
                        conn.commit()
                        logger.info(f"MIDNIGHT ROTATION EXECUTED. Rate limits reset.")
                conn.close()
            except Exception as e:
                logger.error(f"Rotation error: {e}")
            time.sleep(60) # Prevent multiple triggers in same minute
        else:
            time.sleep(30)

def reporting_agent_loop():
    """Generates an executive markdown report daily at 23:59."""
    while True:
        now = datetime.now()
        if now.hour == 23 and now.minute == 59:
            today_str = str(date.today())
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                
                # Fetch daily stats
                cursor.execute("SELECT COUNT(*), SUM(row_count), SUM(payout_amount) FROM data_pipeline WHERE date(timestamp) = ?", (today_str,))
                result = cursor.fetchone()
                total_files = result[0] or 0
                total_rows = result[1] or 0
                total_payout = result[2] or 0.0
                
                cursor.execute("SELECT COUNT(*) FROM data_pipeline WHERE date(timestamp) = ? AND bot_interactions != 'IDLE'", (today_str,))
                bot_queries = cursor.fetchone()[0] or 0
                
                cursor.execute("SELECT daily_upload_bytes FROM system_config WHERE id=1")
                upload_row = cursor.fetchone()
                uploaded_bytes = upload_row[0] if upload_row else 0
                upload_gb = uploaded_bytes / (1024**3)
                
                conn.close()
                
                report_path = os.path.join(REPORTS_DIR, f"Production_Report_{today_str.replace('-','_')}.md")
                with open(report_path, "w", encoding='utf-8') as f:
                    f.write(f"# 📊 Daily Production Report: {today_str}\n\n")
                    f.write(f"### Production Volumes\n")
                    f.write(f"- **Total Files Staged/Uploaded**: {total_files}\n")
                    f.write(f"- **Total Rows Generated**: {total_rows:,}\n")
                    f.write(f"- **Total Data Transmitted**: {upload_gb:.2f} GB\n\n")
                    f.write(f"### Financials & Network\n")
                    f.write(f"- **Bot Inbound Queries Intercepted**: {bot_queries}\n")
                    f.write(f"- **Total Payout Earned**: {total_payout:.2f} USDC\n\n")
                    f.write("> Auto-generated by SRE Management Daemon.\n")
                    
                logger.info(f"Generated Daily Production Report: {report_path}")
            except Exception as e:
                logger.error(f"Report generation error: {e}")
            time.sleep(60)
        else:
            time.sleep(30)

if __name__ == "__main__":
    enforce_os_power_lock()
    
    threading.Thread(target=diagnostic_burn_test_loop, daemon=True).start()
    threading.Thread(target=daily_rotation_loop, daemon=True).start()
    threading.Thread(target=reporting_agent_loop, daemon=True).start()
    
    logger.info("Autonomous SRE Overlay is fully ACTIVE. Listening for system events...")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("SRE Manager shutting down.")
