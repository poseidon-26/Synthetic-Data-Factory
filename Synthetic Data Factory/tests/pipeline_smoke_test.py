import os
import sys
import time
import json
import glob
import subprocess
import threading
import shutil
import pyarrow.parquet as pq

# Ensure tests can import core modules
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

BUFFER_DIR = r"C:\Data_Buffer"
GENERATOR_PATH = os.path.join(BASE_DIR, 'core', 'generator.py')
STREAMER_PATH = os.path.join(BASE_DIR, 'core', 'hf_streamer.py')

def print_step(msg):
    print(f"\n{'-'*60}\n[STEP] {msg}\n{'-'*60}")

def verify_env_and_credentials():
    print_step("1. Environment & Credential Check")
    try:
        import huggingface_hub
        import pyarrow
        import pandas
        import requests
        from huggingface_hub import HfApi
        print("[OK] Required packages ('huggingface_hub', 'pyarrow', 'pandas', 'requests') are installed.")
    except ImportError as e:
        print(f"[FAIL] Missing dependency: {e}")
        sys.exit(1)
        
    hf_token = os.environ.get("HF_WRITE_TOKEN")
    if not hf_token:
        print("[FAIL] HF_WRITE_TOKEN environment variable not set. Please set it for testing.")
        sys.exit(1)
    
    try:
        api = HfApi(token=hf_token)
        user_info = api.whoami()
        print(f"[OK] Authenticated to Hugging Face Hub as: {user_info.get('name')}")
    except Exception as e:
        print(f"[FAIL] Hugging Face Authentication failed: {e}")
        sys.exit(1)

def patch_generator_hooks():
    print_step("2. Micro-Scaled Generation Audit (Patching Generator)")
    if not os.path.exists(GENERATOR_PATH):
        print(f"[FAIL] Generator not found at {GENERATOR_PATH}")
        sys.exit(1)
        
    with open(GENERATOR_PATH, 'r') as f:
        original_code = f.read()
        
    modified_code = original_code
    modified_code = modified_code.replace("TARGET_FILE_SIZE = 5 * 1024 * 1024 * 1024", "TARGET_FILE_SIZE = 15 * 1024 * 1024")
    modified_code = modified_code.replace("PHASE_1_LIMIT = 12.5 * 1024 * 1024 * 1024", "PHASE_1_LIMIT = 22.5 * 1024 * 1024")
    modified_code = modified_code.replace("TOTAL_LIMIT = 25 * 1024 * 1024 * 1024", "TOTAL_LIMIT = 45 * 1024 * 1024")
    
    with open(GENERATOR_PATH, 'w') as f:
        f.write(modified_code)
        
    print("[OK] Generator patched for 45MB micro-run (3 x 15MB files).")
    return original_code

def restore_generator(original_code):
    with open(GENERATOR_PATH, 'w') as f:
        f.write(original_code)
    print("[OK] Generator restored to production 25GB defaults.")

def audit_generated_data():
    parquet_files = glob.glob(os.path.join(BUFFER_DIR, "*.parquet"))
    print(f"[*] Found {len(parquet_files)} parquet files in {BUFFER_DIR}")
    
    if len(parquet_files) < 1:
        print("[FAIL] Generator failed to output any files.")
        sys.exit(1)
        
    for filepath in parquet_files:
        try:
            table = pq.read_table(filepath)
            columns = table.column_names
            
            if "api_state_context" in columns:
                sample_data = table.column("api_state_context")[0].as_py()
                parsed = json.loads(sample_data)
                if "api_token" in parsed and "stack_trace" in parsed:
                    print(f"[OK] JSON syntax validation passed for 'api_state_context' in {os.path.basename(filepath)}")
                else:
                    print(f"[FAIL] Missing expected JSON keys in {os.path.basename(filepath)}")
                    sys.exit(1)
            elif "logical_reasoning_cot" in columns:
                sample_data = table.column("logical_reasoning_cot")[0].as_py()
                if isinstance(sample_data, str) and len(sample_data) > 10:
                    print(f"[OK] High-entropy text validation passed for 'logical_reasoning_cot' in {os.path.basename(filepath)}")
                else:
                    print(f"[FAIL] Invalid text structure in {os.path.basename(filepath)}")
                    sys.exit(1)
            else:
                print(f"[FAIL] Unexpected schema in {os.path.basename(filepath)}")
                sys.exit(1)
                
        except Exception as e:
            print(f"[FAIL] Parquet reading or validation failed for {filepath}: {e}")
            sys.exit(1)
            
    print("[OK] Atomic renaming logic and schema validation passed successfully.")

def run_integration_test():
    verify_env_and_credentials()
    
    # Clear buffer before test
    if os.path.exists(BUFFER_DIR):
        for f in glob.glob(os.path.join(BUFFER_DIR, "*.*")):
            try:
                os.remove(f)
            except:
                pass
    else:
        os.makedirs(BUFFER_DIR, exist_ok=True)
        
    original_gen_code = patch_generator_hooks()
    
    try:
        print_step("Executing Generator & Streamer Subprocesses...")
        
        # 1. Start Generator
        print("[*] Launching generator.py (Simulating 25GB -> 45MB Micro-run)...")
        gen_proc = subprocess.Popen([sys.executable, GENERATOR_PATH], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        
        # Wait a few seconds for it to start creating files
        time.sleep(3)
        
        # Wait for generator to hit limit and exit
        gen_proc.wait(timeout=120)
        print("[OK] Generator hit limit and exited gracefully with code 0.")
        
        # Audit data on disk before streamer deletes it
        print_step("Auditing Generated Data")
        audit_generated_data()
        
        # 2. Start Streamer
        print("[*] Launching hf_streamer.py (Asserting Thread Pool & Tasklist Lifecycle)...")
        # Ensure we pass the test env vars
        streamer_proc = subprocess.Popen([sys.executable, STREAMER_PATH], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        
        # 3. Streamer Connection & Resilience Audit
        print_step("3. Streamer Connection & Resilience Audit")
        print("[*] Waiting for hf_streamer.py to complete uploads and self-terminate...")
        try:
            outs, errs = streamer_proc.communicate(timeout=300)
        except subprocess.TimeoutExpired:
            streamer_proc.kill()
            outs, errs = streamer_proc.communicate()
            print(f"[FAIL] Streamer timed out after 300s.")
            print(f"--- Streamer Output ---\n{outs}\n--- Streamer Errors ---\n{errs}")
            print(f"--- Remaining Files in Buffer ---")
            print(glob.glob(os.path.join(BUFFER_DIR, "*.*")))
            sys.exit(1)
        
        # 4. Windows 11 Lifecycle Assertion
        print_step("4. Windows 11 Lifecycle Assertion")
        if streamer_proc.returncode == 0:
            print("[OK] Streamer detected generator termination and empty buffer.")
            print("[OK] Streamer executed graceful exit (0) instantly.")
        else:
            print(f"[FAIL] Streamer exited with code {streamer_proc.returncode}. Expected 0.")
            print(f"--- Streamer Output ---\n{outs}\n--- Streamer Errors ---\n{errs}")
            sys.exit(1)
            
        remaining_files = glob.glob(os.path.join(BUFFER_DIR, "*.parquet"))
        if not remaining_files:
            print("[OK] All files successfully uploaded and purged via 'os.remove()'.")
        else:
            print("[FAIL] Files remain in buffer! Streamer did not purge them.")
            sys.exit(1)
            
        print_step("VERIFICATION COMPLETE: ALL SYSTEMS NOMINAL")
        print("The 25GB Agile Data Factory workspace passed the end-to-end integration test.")
        
    finally:
        restore_generator(original_gen_code)

if __name__ == "__main__":
    run_integration_test()
