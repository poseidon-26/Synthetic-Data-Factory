import os
import sqlite3
import random
import logging
from fastapi import FastAPI, HTTPException
import uvicorn
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Automated Gateway Router", version="1.0.0")

# Database path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'database', 'pipeline_state.db')

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def simulate_m2m_verification(dataset: dict) -> Dict[str, Any]:
    logger.info(f"Simulating M2M Verification for dataset {dataset['file_name']}")
    
    # 1. Schema validation test
    schema_passed = True
    logger.info(f"[{dataset['file_name']}] Schema validation test: {'PASS' if schema_passed else 'FAIL'}")
    
    # 2. Jensen-Shannon Divergence check
    js_divergence_passed = True
    logger.info(f"[{dataset['file_name']}] Jensen-Shannon Divergence check: {'PASS' if js_divergence_passed else 'FAIL'}")
    
    # 3. High-entropy density threshold validation
    entropy = dataset['entropy_score']
    if entropy is None:
        entropy = 0.0
    entropy_passed = entropy > 0.8
    logger.info(f"[{dataset['file_name']}] High-entropy density threshold validation (entropy={entropy:.4f}): {'PASS' if entropy_passed else 'FAIL'}")
    
    return {
        "schema_validation": schema_passed,
        "js_divergence": js_divergence_passed,
        "high_entropy_density": entropy_passed,
        "entropy_score": entropy
    }

@app.post("/api/v1/query_data")
@app.get("/api/v1/query_data")
def query_data():
    """
    Procurement Bots ping this endpoint to query available datasets.
    The router performs M2M Verification and automated settlement.
    """
    logger.info("Received query_data request from Procurement Bot.")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Read completed datasets that haven't been sold yet
        cursor.execute('''
            SELECT id, file_name, file_path, row_count, entropy_score, upload_status, bot_interactions
            FROM data_pipeline 
            WHERE upload_status = 'COMPLETED' 
              AND (bot_interactions != 'SUCCESS: Dataset Sold' OR bot_interactions IS NULL)
        ''')
        
        datasets = cursor.fetchall()
        
        results = []
        for row in datasets:
            dataset = dict(row)
            
            # Simulate M2M Verification Steps
            verification_results = simulate_m2m_verification(dataset)
            
            # Automated Settlement
            if verification_results["high_entropy_density"]:
                # Calculate payout based on data size (row_count)
                row_count = dataset.get('row_count')
                if row_count and row_count > 0:
                    scale = min(1.0, row_count / 100000.0)
                    payout = 50.00 + (100.00 * scale)
                else:
                    payout = random.uniform(50.00, 150.00)
                
                payout = round(payout, 2)
                
                logger.info(f"[{dataset['file_name']}] Settlement approved. Payout: ${payout} USDC")
                
                # Update database
                cursor.execute('''
                    UPDATE data_pipeline 
                    SET bot_interactions = 'SUCCESS: Dataset Sold',
                        payout_amount = ?
                    WHERE id = ?
                ''', (payout, dataset['id']))
                
                dataset['settlement_status'] = 'SUCCESS: Dataset Sold'
                dataset['payout_usdc'] = payout
            else:
                logger.warning(f"[{dataset['file_name']}] Settlement rejected. Low entropy.")
                # Update database for rejection
                cursor.execute('''
                    UPDATE data_pipeline 
                    SET bot_interactions = 'REJECTED: Low Entropy'
                    WHERE id = ?
                ''', (dataset['id'],))
                
                dataset['settlement_status'] = 'REJECTED: Low Entropy'
                dataset['payout_usdc'] = 0.0
                
            results.append({
                "dataset_id": dataset['id'],
                "file_name": dataset['file_name'],
                "verification": verification_results,
                "settlement": dataset['settlement_status'],
                "payout_usdc": dataset['payout_usdc']
            })
            
        conn.commit()
        conn.close()
        
        return {
            "status": "success",
            "message": f"Processed {len(datasets)} datasets.",
            "data": results
        }
        
    except Exception as e:
        logger.error(f"Error processing query_data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    logger.info("Starting Automated Gateway Router on port 8000...")
    uvicorn.run("router:app", host="0.0.0.0", port=8000, reload=True)
