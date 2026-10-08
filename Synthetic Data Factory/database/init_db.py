import os
import sqlite3
import logging
from datetime import date

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [DB Init] - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def initialize_database():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(base_dir, 'database', 'pipeline_state.db')
    
    logger.info(f"Connecting to database at {db_path}")
    conn = sqlite3.connect(db_path, timeout=20.0)
    
    # Enforce WAL mode and NORMAL synchronous for safe concurrency
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    
    cursor = conn.cursor()
    
    logger.info("Initializing 'data_pipeline' table...")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS data_pipeline (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT UNIQUE,
        file_path TEXT,
        data_category TEXT,
        row_count INTEGER,
        entropy_score REAL,
        upload_status TEXT,
        bot_interactions TEXT DEFAULT 'IDLE',
        payout_amount REAL DEFAULT 0.0,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    logger.info("Initializing 'system_config' table...")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS system_config (
        id INTEGER PRIMARY KEY,
        thermal_throttle INTEGER DEFAULT 0,
        circuit_breaker INTEGER DEFAULT 0,
        active_niche TEXT DEFAULT 'AgenticToolCalling',
        daily_upload_bytes INTEGER DEFAULT 0,
        last_reset_date TEXT
    );
    """)
    
    # Pre-populate defaults
    cursor.execute("SELECT count(*) FROM system_config")
    if cursor.fetchone()[0] == 0:
        logger.info("Pre-populating 'system_config' with default metrics...")
        cursor.execute("""
            INSERT INTO system_config (id, thermal_throttle, circuit_breaker, active_niche, daily_upload_bytes, last_reset_date) 
            VALUES (1, 0, 0, 'AgenticToolCalling', 0, ?)
        """, (str(date.today()),))
    
    conn.commit()
    conn.close()
    logger.info("Database initialization complete.")

if __name__ == "__main__":
    initialize_database()
