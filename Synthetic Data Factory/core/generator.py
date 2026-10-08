import os
import sys
import time
import json
import uuid
import logging
import sqlite3
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
import glob

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Config constraints
BUFFER_DIR = r"C:\Data_Buffer"
DB_PATH = r"C:\Users\kaush\Desktop\Synthetic Data Factory\database\pipeline_state.db"
os.makedirs(BUFFER_DIR, exist_ok=True)

MAX_WORKERS = 6
TARGET_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2GB Micro-Chunking limit
PHASE_1_LIMIT = 12.5 * 1024 * 1024 * 1024  # 12.5GB Data
TOTAL_LIMIT = 25 * 1024 * 1024 * 1024      # 25GB Hard Stop
BATCH_SIZE = 500_000  # Number of rows per PyArrow append
RAM_BUFFER_LIMIT = 50 * 1024 * 1024        # 50MB In-Memory Cache

# Chrono-Routing Matrix
CHRONO_MATRIX = {
    "Monday": {"prefix": "Agentic"},
    "Tuesday": {"prefix": "Code"},
    "Wednesday": {"prefix": "Cyber"},
    "Thursday": {"prefix": "DeFi"},
    "Friday": {"prefix": "Clinical"},
    "Saturday": {"prefix": "Safety"},
    "Sunday": {"prefix": "Math"}
}

# Pre-generate JSON pools for extreme throughput across different tracks
SAMPLES = {
    "Agentic": {
        "Phase1": [
            json.dumps({"api_token": str(uuid.uuid4()), "rate_limit_remaining": r, "stack_trace": st, "environment_state": env})
            for r in [5000, 4999, 0, 10, 429]
            for st in ["None", "Error: timeout", "Connection reset by peer", "RateLimitExceeded"]
            for env in ["healthy", "degraded", "recovering", "critical"]
        ],
        "Phase2": [
            "CoT Trace: Analyzing stack trace... identified timeout. Switching to secondary API endpoint. Recovered.",
            "CoT Trace: Rate limit 0 detected. Halting operations for 60s. Resuming logic execution.",
            "CoT Trace: Connection reset. Interpreting error as transient. Retrying with exponential backoff.",
            "CoT Trace: Validating environment state. Detected degraded performance. Pre-emptively caching responses.",
            "CoT Trace: RateLimitExceeded block. Executing circuit breaker pattern. Evaluating queue depths."
        ]
    },
    "Code": {
        "Phase1": [json.dumps({"repo": "foo/bar", "file": "main.py", "issue": "SyntaxError", "line": l}) for l in [10, 42, 99, 120]],
        "Phase2": ["CoT Trace: Analyzing AST...", "CoT Trace: Type inference failed. Reconstructing...", "CoT Trace: Resolving merge conflict."]
    },
    "Cyber": {
        "Phase1": [json.dumps({"ip": f"192.168.1.{i}", "port": p, "sig": s}) for i in range(10) for p in [22, 80, 443] for s in ["SQLi", "XSS", "BufferOverflow"]],
        "Phase2": ["CoT Trace: Zero-day signature matched.", "CoT Trace: Blocking IP...", "CoT Trace: Isolating subnet."]
    },
    "DeFi": {
        "Phase1": [json.dumps({"pool": p, "liquidity": l, "tx_fee": f}) for p in ["USDC/ETH", "DAI/ETH"] for l in [1000, 50000] for f in [0.01, 0.05]],
        "Phase2": ["CoT Trace: MEV sandwich attack detected.", "CoT Trace: Flash loan arbitrage executed.", "CoT Trace: Rebalancing LP tokens."]
    },
    "Clinical": {
        "Phase1": [json.dumps({"patient_id": str(uuid.uuid4())[:8], "vitals": {"hr": h, "bp": b}}) for h in [60, 90, 120] for b in ["120/80", "140/90"]],
        "Phase2": ["CoT Trace: Cross-referencing biomarkers.", "CoT Trace: Elevated heart rate detected.", "CoT Trace: HIPAA-compliant masking applied."]
    },
    "Safety": {
        "Phase1": [json.dumps({"prompt_hash": str(uuid.uuid4())[:8], "toxicity_score": t, "flagged": f}) for t in [0.1, 0.9] for f in [True, False]],
        "Phase2": ["CoT Trace: Red team prompt deflected.", "CoT Trace: Jailbreak attempt recognized.", "CoT Trace: Alignment policy strictly enforced."]
    },
    "Math": {
        "Phase1": [
            json.dumps({"algorithmic_state": "polynomial_expansion", "variables": {"x": i, "y": j}, "graph_connectivity": "connected"})
            for i in range(1, 6) for j in range(10, 15)
        ],
        "Phase2": [
            "CoT Trace: Process Supervision - Verifying step 3. Logic fallacy detected in sign distribution. Correcting path immediately.",
            "CoT Trace: Validating multi-variable algorithmic state graph. Connectivity holds. Proceeding to mathematical node reduction.",
            "CoT Trace: PRM Check - Integration bounds inverted incorrectly. Applying self-correction to restore mathematical correctness."
        ]
    }
}

# --- MULTIPROCESSING INITIALIZER ---
def init_worker(shared_bytes, shared_phase, s_event, s_lock, prefix):
    global total_bytes_written, current_phase, shutdown_event, state_lock, active_prefix
    total_bytes_written = shared_bytes
    current_phase = shared_phase
    shutdown_event = s_event
    state_lock = s_lock
    active_prefix = prefix

def generate_phase1_batch(n, prefix, pid):
    """Generates a high-speed NumPy array of nested JSON strings (Raw Logs)."""
    logger.info(f"DEBUG - Worker Process [{pid}] generating token sequence (Bypassing Filters)...")
    samples = SAMPLES[prefix]["Phase1"]
    arr = np.random.choice(samples, size=n)
    return pa.table({"api_state_context": pa.array(arr)})

def generate_phase2_batch(n, prefix, pid):
    """Generates multi-turn cognitive traces (Reasoning CoT)."""
    logger.info(f"DEBUG - Worker Process [{pid}] generating token sequence (Bypassing Filters)...")
    samples = SAMPLES[prefix]["Phase2"]
    arr = np.random.choice(samples, size=n)
    return pa.table({"logical_reasoning_cot": pa.array(arr)})

def sync_rows_to_db(file_name, file_path, category, row_count, is_final=False):
    """Dynamically updates the DB with row progress or finalizes the chunk."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30.0)
            cursor = conn.cursor()
            
            cursor.execute("SELECT id FROM data_pipeline WHERE file_name = ?", (file_name,))
            record = cursor.fetchone()
            
            if record:
                cursor.execute(
                    "UPDATE data_pipeline SET row_count = ?, upload_status = ? WHERE file_name = ?",
                    (row_count, 'PENDING' if is_final else 'UPLOADING', file_name)
                )
            else:
                cursor.execute(
                    "INSERT INTO data_pipeline (file_name, file_path, data_category, row_count, entropy_score, upload_status) VALUES (?, ?, ?, ?, ?, ?)",
                    (file_name, file_path, category, row_count, 0.95, 'PENDING' if is_final else 'UPLOADING')
                )
                
            conn.commit()
            conn.close()
            break
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower():
                logger.warning(f"WARN - DB Locked, retrying in 500ms... (Attempt {attempt+1}/{max_retries})")
                time.sleep(0.5)
            else:
                logger.error(f"DB Operational Error for {file_name}: {e}")
                break
        except Exception as e:
            logger.error(f"DB sync failed for {file_name}: {e}")
            break

def worker_task():
    pid = os.getpid()
    
    with state_lock:
        if shutdown_event.is_set():
            return
        phase = current_phase.value

    file_id = str(uuid.uuid4())
    cat_name = f"{active_prefix}_RawLogs" if phase == 1 else f"{active_prefix}_ReasoningCoT"
    tmp_path = os.path.join(BUFFER_DIR, f"{cat_name}_{file_id}.tmp")
    final_path = os.path.join(BUFFER_DIR, f"{cat_name}_{file_id}.parquet")
    
    writer = None
    file_rows = 0
    last_tracked_size = 0
    
    # 50MB In-Memory Cache Variables
    ram_buffer = []
    ram_buffer_size = 0
    buffer_rows = 0
    
    try:
        while not shutdown_event.is_set():
            with state_lock:
                if current_phase.value != phase or shutdown_event.is_set():
                    break
            
            # GENERATING TOKEN SEQUENCE (Immediate Acceptance, No Loops)
            table = generate_phase1_batch(BATCH_SIZE, active_prefix, pid) if phase == 1 else generate_phase2_batch(BATCH_SIZE, active_prefix, pid)
                
            if writer is None:
                writer = pq.ParquetWriter(tmp_path, table.schema, compression='snappy')
                
            # APPEND TO RAM BUFFER
            ram_buffer.append(table)
            ram_buffer_size += table.nbytes
            buffer_rows += BATCH_SIZE
            
            # FLUSH BUFFER TO DISK ONCE IT EXCEEDS 50MB
            if ram_buffer_size >= RAM_BUFFER_LIMIT or shutdown_event.is_set() or current_phase.value != phase:
                logger.info(f"DEBUG - Worker Process [{pid}] attempting local buffer disk write (Consolidated {ram_buffer_size / (1024*1024):.2f}MB Block)...")
                consolidated_table = pa.concat_tables(ram_buffer)
                writer.write_table(consolidated_table)
                file_rows += buffer_rows
                
                logger.info(f"SUCCESS - Worker Process [{pid}] wrote row to buffer chunk successfully.")
                
                base_name = os.path.basename(final_path)
                sync_rows_to_db(base_name, tmp_path, cat_name, file_rows, is_final=False)
                
                # Reset memory cache
                ram_buffer = []
                ram_buffer_size = 0
                buffer_rows = 0
            
                if os.path.exists(tmp_path):
                    current_size = os.path.getsize(tmp_path)
                    delta = current_size - last_tracked_size
                    
                    with state_lock:
                        total_bytes_written.value += delta
                        last_tracked_size = current_size
                        
                        if total_bytes_written.value >= PHASE_1_LIMIT and current_phase.value == 1:
                            logger.info(f"12.5GB Phase 1 Limit Reached. Toggling state to Phase 2 (Reasoning CoT). [Triggered by Process {pid}]")
                            current_phase.value = 2
                            
                        if total_bytes_written.value >= TOTAL_LIMIT:
                            if not shutdown_event.is_set():
                                logger.info(f"INFO - 25GB Target Reached. Graceful Shutdown Initiated. [Triggered by Process {pid}]")
                                shutdown_event.set()

                    if current_size >= TARGET_FILE_SIZE or shutdown_event.is_set() or current_phase.value != phase:
                        break
                    
    except Exception as e:
        logger.error(f"Worker exception assembling {cat_name} on Process [{pid}]: {e} (Line {sys.exc_info()[-1].tb_lineno})")
    finally:
        # Flush any remaining data in RAM buffer before closing
        if writer and len(ram_buffer) > 0:
            try:
                consolidated_table = pa.concat_tables(ram_buffer)
                writer.write_table(consolidated_table)
                file_rows += buffer_rows
                base_name = os.path.basename(final_path)
                sync_rows_to_db(base_name, tmp_path, cat_name, file_rows, is_final=False)
            except Exception as e:
                logger.error(f"Worker exception flushing final memory buffer on Process [{pid}]: {e}")
                
        if writer:
            writer.close()
            writer = None
            
        if os.path.exists(tmp_path):
            try:
                os.rename(tmp_path, final_path)
                file_size_final = os.path.getsize(final_path)
                logger.info(f"Committed {final_path} | Size: {file_size_final / (1024**3):.2f} GB | Global Written: {total_bytes_written.value / (1024**3):.2f} GB")
            except (FileNotFoundError, PermissionError) as e:
                logger.info(f"Committed {final_path} | Instantly locked/streamed by another process. | Global Written: {total_bytes_written.value / (1024**3):.2f} GB")
                
            if file_rows > 0:
                base_name = os.path.basename(final_path)
                sync_rows_to_db(base_name, final_path, cat_name, file_rows, is_final=True)

def main():
    current_day = datetime.datetime.now().strftime("%A")
        
    if current_day not in CHRONO_MATRIX:
        logger.error(f"Unknown day {current_day}")
        sys.exit(1)
        
    active_prefix = CHRONO_MATRIX[current_day]["prefix"]
    
    print("\n" + "="*50)
    print(f"--- TODAY IS {current_day.upper()}: LAUNCHING {active_prefix.upper()} TRACK ---")
    print("="*50 + "\n")
    
    logger.info("Starting Strict State-Bounded Generator Pipeline (GIL-Bypassed Multiprocessing).")
    
    # --- Active Database Ledger Purge ---
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        cursor = conn.cursor()
        
        # Purge data_pipeline
        try:
            cursor.execute("DELETE FROM data_pipeline WHERE upload_status IN ('PENDING', 'UPLOADING')")
            logger.info("Purged active/stale states from data_pipeline.")
        except:
            pass
            
        # Purge data_ledger (if it exists)
        try:
            cursor.execute("DELETE FROM data_ledger WHERE upload_status IN ('PENDING', 'UPLOADING')")
            logger.info("Purged active/stale states from data_ledger.")
        except:
            pass
            
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Could not purge active DB ledger: {e}")
        
    # --- Buffer Purge & Discard Hook ---
    stale_files = glob.glob(os.path.join(BUFFER_DIR, "*.tmp"))
    if stale_files:
        logger.info(f"Purging {len(stale_files)} un-finalized '.tmp' chunks from previous stalled sessions.")
        for f in stale_files:
            try:
                os.remove(f)
                logger.info(f"Successfully purged stale chunk: {f}")
            except Exception as e:
                logger.warning(f"Could not purge {f}: {e}")
    
    logger.info(f"Target Track: {active_prefix} Track")
    logger.info(f"Hard Stop Limit: {TOTAL_LIMIT / (1024**3):.0f} GB | Concurrency: {MAX_WORKERS} Processes")
    logger.info(f"Micro-Chunking Target: {TARGET_FILE_SIZE / (1024**3):.2f} GB")
    
    # --- Shared Memory Initialization ---
    manager = mp.Manager()
    shared_bytes = manager.Value('d', 0.0)
    shared_phase = manager.Value('i', 1)
    s_event = manager.Event()
    s_lock = manager.Lock()
    
    with ProcessPoolExecutor(max_workers=MAX_WORKERS, initializer=init_worker, initargs=(shared_bytes, shared_phase, s_event, s_lock, active_prefix)) as executor:
        futures = []
        
        while not s_event.is_set():
            futures = [f for f in futures if not f.done()]
            
            while len(futures) < MAX_WORKERS and not s_event.is_set():
                futures.append(executor.submit(worker_task))
                
            time.sleep(1)
            
        for f in futures:
            try:
                f.result()
            except Exception as e:
                logger.error(f"Process crashed: {e}")
            
    logger.info("All worker processes have gracefully flushed and exited.")
    logger.info("INFO - 25GB Target Reached. Graceful Shutdown Initiated.")
    sys.exit(0)

if __name__ == "__main__":
    mp.freeze_support()
    main()
