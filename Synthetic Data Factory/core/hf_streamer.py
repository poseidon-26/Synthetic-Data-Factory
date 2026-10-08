import os
import sys
import time
import glob
import logging
import gc
import sqlite3
import datetime
from concurrent.futures import ThreadPoolExecutor
from getpass import getpass
from huggingface_hub import HfApi

# Setup production logging framework
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Absolute Configuration Constraints
BUFFER_DIR = r"C:\Data_Buffer"
MAX_WORKERS = 3

CHRONO_MATRIX = {
    "Monday": {"repo": "agentic-tool-recovery-pool"},
    "Tuesday": {"repo": "code-synthesis-debug-pool"},
    "Wednesday": {"repo": "cybersec-zeroday-defense-pool"},
    "Thursday": {"repo": "defi-quant-mev-pool"},
    "Friday": {"repo": "clinical-diagnostics-pool"},
    "Saturday": {"repo": "safety-redteaming-pool"},
    "Sunday": {"repo": "math-prm-algorithmic-pool"}
}

def setup_auth():
    """Prompt once for HF credentials if env vars are missing."""
    token = os.environ.get("HF_WRITE_TOKEN")
    
    if not token:
        try:
            # Try to read from token file in root directory or current directory
            token_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "hf_token.txt")
            if not os.path.exists(token_file):
                token_file = "hf_token.txt"
            
            with open(token_file, "r") as f:
                token = f.read().strip()
        except Exception:
            pass

    if not token:
        print("\n--- Hugging Face Authentication ---")
        token = getpass("Enter HF_WRITE_TOKEN: ").strip()
        
    return token

def is_generator_terminated():
    """Detects generator process termination via Windows wmic."""
    try:
        output = os.popen('wmic process where "name=\'python.exe\'" get commandline').read()
        if "generator.py" in output:
            return False
        return True
    except Exception as e:
        logger.warning(f"Process check failed, assuming generator is still active: {e}")
        return False

def upload_task(file_path, api, repo_id):
    """
    Safely executes the multi-threaded chunk upload with Deep Fault Tolerance.
    Implements exponential backoff and precise file-lock exception handling.
    """
    if not os.path.exists(file_path):
        return

    file_name = os.path.basename(file_path)
    path_in_repo = f"data/{file_name}"
    max_retries = 3
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Stream-Uploading {file_name} (Attempt {attempt}/{max_retries})...")
            
            # Using path_or_fileobj directly leverages HfApi's internal C-level stream buffer
            api.upload_file(
                path_or_fileobj=file_path,
                path_in_repo=path_in_repo,
                repo_id=repo_id,
                repo_type="dataset"
            )
            
            logger.info(f"Upload 100% Successful: {file_name}")
            
            # Update Database Ledger
            try:
                db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'database', 'pipeline_state.db')
                conn = sqlite3.connect(db_path, timeout=20.0)
                conn.execute("UPDATE data_pipeline SET upload_status = 'COMPLETED', timestamp = CURRENT_TIMESTAMP WHERE file_name = ?", (file_name,))
                conn.commit()
                conn.close()
                logger.info(f"Database ledger successfully updated for {file_name}")
            except Exception as db_e:
                logger.error(f"Ledger update failed for {file_name}: {db_e}")
            
            # Post-Upload Wipeout: Force explicit garbage collection and instantly clear disk space
            gc.collect()
            os.remove(file_path)
            logger.info(f"Disk Cleared: {file_name} has been purged from buffer.")
            break
            
        except (FileNotFoundError, PermissionError) as e:
            # File got deleted, purged, or locked externally
            logger.warning(f"WARN - File missing or inaccessible, skipping to next chunk... ({e})")
            break
            
        except Exception as e:
            if attempt < max_retries:
                backoff = 2 ** attempt
                logger.warning(f"Network fluctuation on {file_name}: {e}. Retrying in {backoff}s...")
                time.sleep(backoff)
            else:
                logger.error(f"Upload failed for {file_name} after {max_retries} attempts.")

def main():
    logger.info("Initializing Hugging Face Streamer...")
    
    current_day = datetime.datetime.now().strftime("%A")
        
    if current_day not in CHRONO_MATRIX:
        logger.error(f"Unknown day {current_day}")
        sys.exit(1)
        
    repo_suffix = CHRONO_MATRIX[current_day]["repo"]
    repo_id = f"Posiedon26/{repo_suffix}"
    
    # 1. Requirements & Auth
    token = setup_auth()
    api = HfApi(token=token)
    
    try:
        # Ensure the dataset repository exists
        api.create_repo(repo_id=repo_id, repo_type="dataset", private=True, exist_ok=True)
        logger.info(f"Target dataset repository confirmed: {repo_id}")
        
        # Inject premium metadata tags into the dynamic README.md for Sunday
        if current_day == "Sunday":
            import io
            readme_content = (
                "---\n"
                "tags:\n"
                "- chain-of-thought\n"
                "- rlhf\n"
                "- process-supervision\n"
                "- mathematical-reasoning\n"
                "- prm\n"
                "- high-entropy\n"
                "---\n"
                "# Math PRM Algorithmic Pool\n\n"
                "High-entropy mathematical execution states and deep process supervision traces."
            )
            try:
                api.upload_file(
                    path_or_fileobj=io.BytesIO(readme_content.encode('utf-8')),
                    path_in_repo="README.md",
                    repo_id=repo_id,
                    repo_type="dataset"
                )
                logger.info("INFO - Injected premium PRM metadata into README.md for Sunday track.")
            except Exception as metadata_e:
                logger.warning(f"Failed to inject README metadata: {metadata_e}")
                
    except Exception as e:
        logger.warning(f"Repo validation warning: {e}")

    logger.info(f"Engaging ThreadPoolExecutor with MAX_WORKERS = {MAX_WORKERS}")
    
    # 2. Concurrency Control
    processing_files = set()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        active_futures = set()
        
        while True:
            # Garbage collect completed futures
            done = {fut for fut in active_futures if fut.done()}
            active_futures.difference_update(done)
            
            # Scan landing zone for Parquet files
            parquet_files = glob.glob(os.path.join(BUFFER_DIR, "*.parquet"))
            
            for file_path in parquet_files:
                if file_path not in processing_files and len(active_futures) < MAX_WORKERS:
                    processing_files.add(file_path)
                    
                    # Wrapper to discard from tracking set upon completion
                    def task_wrapper(fp=file_path):
                        try:
                            upload_task(fp, api, repo_id)
                        finally:
                            processing_files.discard(fp)
                            
                    future = executor.submit(task_wrapper)
                    active_futures.add(future)
            
            # 5. Smart Termination (Graceful Exit)
            if not parquet_files and not active_futures:
                # Buffer is empty of parquets. Check if directory is entirely empty of .tmp files too
                is_dir_empty = len(os.listdir(BUFFER_DIR)) == 0
                
                if is_dir_empty and is_generator_terminated():
                    logger.info("INFO - Ingestion Complete. All chunks securely anchored to Hugging Face. Graceful shutdown executed.")
                    break
                    
            # Cycle breather to prevent CPU spin
            time.sleep(2)

if __name__ == "__main__":
    main()
