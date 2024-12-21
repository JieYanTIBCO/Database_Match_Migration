import logging
from traceback import print_tb
from sqlite3_db_operations import *
import sqlite3

# Logging Configuration
logging.basicConfig(
    handlers=[RotatingFileHandler('migration.log', maxBytes=5 * 1024 * 1024, backupCount=3),  # 5 MB per file, keep 3 backups
              logging.StreamHandler()  # Log to the console
              ])

if __name__ == "__main__":

    DB_PATH = "example.db"
    SOURCE_TABLE = 'customer_mig'
    TARGET_TABLE = 'customer'
    BATCH_SIZE = 2000

    # Step 1: Fetch existing primary keys from target table
    existing_keys = fetch_primary_keys(DB_PATH, TARGET_TABLE)

    # Step 2: Process source table and insert missing records into target table
    process_and_insert_missing_records(
        DB_PATH, SOURCE_TABLE, TARGET_TABLE, existing_keys, BATCH_SIZE)
