from duckdb_operations import *
from concurrent.futures import ThreadPoolExecutor



if __name__ == "__main__":
    # Constants
    DB_PATH = 'example.duckdb'
    TARGET_TABLE = 'customer'  # 修改后的变量名
    MIG_TABLE = 'mig_table'
    SQL_FILE_PATH = 'create_customer.sql'
    NUM_RECORDS = 20000
    NUM_THREADS = 4
    BATCH_SIZE = 2000
    PERCENTAGE = 50

    try:
        # Step 0: Cleanup DuckDB
        logger.info("Cleaning up DuckDB...")
        with duckdb.connect(DB_PATH) as conn:
            # Drop all existing tables
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';").fetchall()
            for table_name in tables:
                conn.execute(f"DROP TABLE IF EXISTS {table_name[0]}")
                logger.info(f"Dropped table: {table_name[0]}")
            logger.info("DuckDB cleanup completed.")

        # Step 1: Create the table
        logger.info("Creating table...")
        create_table(DB_PATH, TARGET_TABLE, SQL_FILE_PATH)  # 使用修改后的变量
        create_table(DB_PATH, MIG_TABLE, SQL_FILE_PATH)  # 使用修改后的变量

        # Step 2 and 3 combined: Generate and insert records
        logger.info(
            f"Generating and inserting {NUM_RECORDS} mock records using {NUM_THREADS} threads...")
        generate_and_insert_records(
            DB_PATH, TARGET_TABLE, NUM_RECORDS, NUM_THREADS, BATCH_SIZE  # 使用修改后的变量
        )

        logger.info("Process completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

    verify_tables(DB_PATH, TARGET_TABLE)  # 使用修改后的变量

    try:
        # Call the migration function
        migrate_records_with_percentage_batch(
            DB_PATH, TARGET_TABLE, MIG_TABLE, PERCENTAGE, BATCH_SIZE, NUM_THREADS
        )
    except Exception as e:
        logger.error(f"Migration failed: {e}")
