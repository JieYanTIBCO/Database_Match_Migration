import logging
from traceback import print_tb
from duckdb_operations import *
import sqlite3

# Logging Configuration
logging.basicConfig(
    handlers=[RotatingFileHandler('migration.log', maxBytes=5 * 1024 * 1024, backupCount=3),  # 5 MB per file, keep 3 backups
              logging.StreamHandler()  # Log to the console
              ])

if __name__ == "__main__":

    DB_PATH = 'example.duckdb'
    TABLE_NAME = "customer"
    TARGET_TABLE = 'customer'  # 修改后的变量名
    MIG_TABLE = 'mig_table'
    SQL_FILE_PATH = 'create_customer.sql'
    NUM_RECORDS = 200000
    NUM_THREADS = 4
    BATCH_SIZE = 2000
    verify_tables(DB_PATH, TABLE_NAME)
    verify_tables(DB_PATH, MIG_TABLE)
