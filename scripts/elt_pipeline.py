import pandas as pd
import os
import shutil
import logging
import datetime
from pathlib import Path
import psycopg2
from sqlalchemy import create_engine, text

# Import your local generator script
from data_generator import generate_daily_batch
from config.db_config import DB_CONFIG

# Construct Connection String using the imported credentials
DB_CONN_STR = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

# 2. File Paths
BASE_DIR = Path(__file__).parent.parent 
DATA_DIR = BASE_DIR / "data"
SQL_DIR = BASE_DIR / "SQL queries"
LOG_DIR = BASE_DIR / "logs"

SQL_FILES = [
    SQL_DIR / "03_daily_batch_load.sql",    
    SQL_DIR / "04_analytics_marts.sql" 
]

# 3. Setup Logging
# This block tells Python: "Write everything to a file named pipeline_execution.log"
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=LOG_DIR / "pipeline_execution.log",
    level=logging.INFO, # We want to capture everything from INFO level and up (INFO, WARNING, ERROR)
    format='%(asctime)s - %(levelname)s - %(message)s', # Format: "2025-01-01 02:00:00 - INFO - Pipeline Started"
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Also show logs in the terminal so we can watch it run
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)


# ------------------- PIPELINE STEPS ---------------

# Step 1: Getting the data from oltp (for this project we are simulating the oltp data)
def step_1_generate_data():
    logging.info("--- [STEP 1] Generating Daily Data ---")
    
    try:
        simulation_date = datetime.date(2024,1,3)
        generate_daily_batch(simulation_date)
        logging.info(f"Data generation complete for {simulation_date}")
    except Exception as e:
        logging.error(f"Generation Failed: {e}")
        raise e 

def step_2_load_raw(engine):
    logging.info("--- [STEP 2] Loading Raw Data to Postgres ---")
    incoming_files = list((DATA_DIR / "daily_incoming_data").glob("*.csv"))

    try:
        # Check if there is any file in incoming folder
        if not incoming_files:
            logging.warning("No new files found in data/incoming. Skipping load.")
            return None

        # Get the latest file
        latest_file = max(incoming_files, key=os.path.getctime)
        logging.info(f"Processing file: {latest_file.name}")

        df = pd.read_csv(latest_file)
        
        df.to_sql(
            name='inventory_dump',
            con=engine,
            schema='raw',
            if_exists='append', 
            index=False,
            method='multi',
            chunksize=1000 
        )
        logging.info(f"Successfully loaded {len(df)} rows to raw.inventory_dump")
        return latest_file

    except Exception as e:
        logging.error(f"Raw Load Failed: {e}")
        raise e

def step_3_run_transformations(engine):
    logging.info("--- [STEP 3] Running SQL Transformations ---")
    
    try:
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT") # preventing 
            
            for sql_path in SQL_FILES:
                if not sql_path.exists():
                    logging.error(f"SQL file missing: {sql_path}")
                    continue
                
                logging.info(f"Executing: {sql_path.name}")
                with open(sql_path, 'r') as f:
                    sql_script = f.read()
                    
                conn.execute(text(sql_script))
                logging.info(f"Finished: {sql_path.name}")
                
    except Exception as e:
        logging.error(f"Transformation Failed: {e}")
        raise e

def step_4_archive_file(file_path):
    # Moves the file to archive so we don't process it twice.
    if not file_path: return

    logging.info("--- [STEP 4] Archiving File ---")
    archive_dir = DATA_DIR / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        destination = archive_dir / file_path.name
        shutil.move(str(file_path), str(destination))
        logging.info(f"Moved {file_path.name} to {archive_dir}")
    except Exception as e:
        logging.error(f"Archiving Failed: {e}")


def step_5_updating_analytics_tables(engine):
    logging.info("--- [STEP 5] Updating Analytics Tables ---")
    
    try:
        query_tables_list = """ 
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'rpt' 
        """

        rpt_tables = pd.read_sql(query_tables_list, DB_CONN_STR)
        dest_dirt = BASE_DIR / "dashboard" / "data"
        dest_dirt.mkdir(parents=True, exist_ok=True)

        for name in rpt_tables.iloc[:,0]:
            path = dest_dirt / (name + ".csv")
            table = pd.read_sql_table(name, engine, "rpt")
            table.to_csv(path)

        logging.info(f"Updated reporting tables")

    except Exception as e:
        logging.error(f"Updating reporting tables failed: {e}")

# MAIN

def main():
    logging.info("=== ETL PIPELINE START ===")
    
    try:
        engine = create_engine(DB_CONN_STR)

        step_1_generate_data()
        
        processed_file = step_2_load_raw(engine)
        
        if processed_file:
            step_3_run_transformations(engine)
            step_4_archive_file(processed_file)
            # step_5_updating_analytics_tables(engine)
            
        logging.info("=== ETL PIPELINE SUCCESS ===")
        
    except Exception as e:
        # LOGGING: CRITICAL means the pipeline died completely. 
        logging.critical(f"=== ETL PIPELINE CRASHED ===\nError: {e}")
        exit(1)

if __name__ == "__main__":
    main()