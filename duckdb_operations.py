from logging.handlers import RotatingFileHandler
import threading
import uuid
import duckdb
import random
import string
from datetime import datetime, timedelta
import os
import logging
import concurrent.futures
from faker import Faker
from concurrent.futures import ThreadPoolExecutor, as_completed

# Logging Configuration
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s",
    handlers=[
        # 5 MB per file, keep 3 backups
        RotatingFileHandler('db_operation.log', maxBytes=5 * \
                            1024 * 1024, backupCount=3),
        logging.StreamHandler()  # Log to the console
    ]
)

logger = logging.getLogger(__name__)
fake = Faker('en_US')  # Initialize Faker only once
logging.getLogger('faker').setLevel(logging.INFO)

# Global Counter
global_batch_counter = 0
batch_lock = threading.Lock()  # For updating the counter safely


def sanitize_table_name(table_name):
    """
    Validate and sanitize table names to prevent SQL injection.
    """
    if not table_name.isidentifier():
        raise ValueError(f"Invalid table name: {table_name}")
    return table_name


def create_table(db_path, table_name, sql_file_path):
    """
    Create a table in the DuckDB database based on a schema from a SQL file.

    Parameters:
        db_path (str): Path to the DuckDB database file.
        table_name (str): Name of the table to create.
        sql_file_path (str): Path to the SQL file containing the table schema.
    """
    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

    with open(sql_file_path, 'r') as file:
        create_table_ddl = file.read()

    create_table_ddl = create_table_ddl.replace(
        "{TABLE_NAME}", sanitize_table_name(table_name))

    try:
        with duckdb.connect(db_path) as conn:
            conn.execute(create_table_ddl)
            logger.info(f"Table '{table_name}' created successfully.")
    except Exception as e:
        logger.error(f"Error creating table '{table_name}': {e}")
        raise


def generate_mock_record(unique_ids):
    """
    Generate a single mock record for the customer table with a UUID primary key.

    Parameters:
        unique_ids (set): A set to store and check for duplicate customer IDs.

    Returns:
        tuple: A tuple representing a record (customer_id, name, gender, DOB, Nationality).
    """
    while True:
        # Generate realistic random data
        customer_id = str(uuid.uuid4())  # Unique UUID
        name = fake.name()  # Realistic name
        gender = random.choice(['M', 'F', 'O'])  # Random gender
        DOB = fake.date_of_birth(minimum_age=18, maximum_age=70).strftime(
            "%Y-%m-%d")  # Realistic DOB
        nationality = fake.country()  # Realistic country name

        # Ensure uniqueness of customer_id
        if customer_id not in unique_ids:
            unique_ids.add(customer_id)
            return (customer_id, name, gender, DOB, nationality)


def insert_records_in_batches(db_path, table_name, records, batch_size):
    """
    Insert records into a DuckDB table in batches, with global batch tracking.
    """
    global global_batch_counter

    try:
        with duckdb.connect(db_path) as conn:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                insert_query = f"""
                INSERT INTO {table_name} (customer_id, name, gender, DOB, Nationality)
                VALUES (?, ?, ?, ?, ?);
                """
                conn.executemany(insert_query, batch)

                # Update global counter
                with batch_lock:
                    global_batch_counter += 1
                    logger.info(
                        f"Batch {global_batch_counter} processed with {len(batch)} records.")

            logger.info(f"All records in this thread inserted successfully.")
    except Exception as e:
        logger.error(f"Error inserting records into table '{table_name}': {e}")
        raise


def generate_and_insert_records(db_path, table_name, total_records, num_threads, batch_size):
    """
    Generate and insert mock records into a DuckDB table using threads, with batch progress logging.
    """
    unique_ids = set()

    def generate_and_insert_partial(record_count):
        """Generate a partial set of records and insert them."""
        records = [generate_mock_record(unique_ids)
                   for _ in range(record_count)]
        logger.debug(
            f"Generated {len(records)} records.")
        insert_records_in_batches(db_path, table_name, records, batch_size)

    # Calculate how many records each thread should process
    records_per_thread = total_records // num_threads
    remaining_records = total_records % num_threads

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit tasks for each thread
        futures = [executor.submit(generate_and_insert_partial, records_per_thread)
                   for _ in range(num_threads)]

        # Handle remaining records in one extra thread, if needed
        if remaining_records > 0:
            futures.append(executor.submit(
                generate_and_insert_partial, remaining_records))

        # Wait for all threads to complete
        for future in futures:
            future.result()

    logger.info(
        f"Generated and inserted {total_records} records into '{table_name}' successfully.")


def migrate_records_with_percentage_batch(db_path, org_table, mig_table, percentage, batch_size, num_threads=4):
    """
    Migrate a percentage of records from one DuckDB table to another using multiple threads, avoiding duplicates.

    Parameters:
        db_path (str): Path to the DuckDB database file.
        org_table (str): Source table.
        mig_table (str): Destination table.
        percentage (float): Percentage of records to migrate.
        batch_size (int): Number of records to process per batch.
        num_threads (int): Number of threads to use for migration.
    """
    try:
        with duckdb.connect(db_path) as conn:
            # Step 1: Calculate total records and migration target
            total_records = conn.execute(
                f"SELECT COUNT(*) FROM {sanitize_table_name(org_table)}"
            ).fetchone()[0]
            migrate_count = int(total_records * (percentage / 100.0))
            logger.info(f"Total records in '{org_table}': {total_records}")
            logger.info(
                f"Migrating {migrate_count} records ({percentage}% of total) to '{mig_table}'.")

            # Step 2: Fetch existing IDs from mig_table to avoid duplicates
            existing_ids = set(record[0] for record in conn.execute(
                f"SELECT customer_id FROM {sanitize_table_name(mig_table)}"
            ).fetchall())
            logger.info(
                f"Found {len(existing_ids)} existing IDs in '{mig_table}'.")

        # Step 3: Initialize threading
        offset_batches = [(offset, batch_size)
                          for offset in range(0, migrate_count, batch_size)]
        migrated_records = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            future_to_offset = {
                executor.submit(
                    migrate_batch,
                    db_path, org_table, mig_table, offset, batch_size, existing_ids
                ): offset for offset, batch_size in offset_batches
            }

            for future in as_completed(future_to_offset):
                result = future.result()  # Get the migrated record count for this batch
                migrated_records += result
                logger.info(
                    f"Total migrated so far: {migrated_records}/{migrate_count}")

        logger.info(
            f"Successfully migrated {migrated_records} records into '{mig_table}'.")
    except Exception as e:
        logger.error(
            f"Error during migration from '{org_table}' to '{mig_table}': {e}")
        raise


def migrate_batch(db_path, org_table, mig_table, offset, batch_size, existing_ids):
    """
    Migrate a batch of records from org_table to mig_table.

    Parameters:
        db_path (str): Path to the DuckDB database file.
        org_table (str): Source table.
        mig_table (str): Destination table.
        offset (int): Offset to fetch records from.
        batch_size (int): Number of records in the batch.
        existing_ids (set): Existing IDs in the destination table.

    Returns:
        int: Number of records migrated in this batch.
    """
    try:
        with duckdb.connect(db_path) as conn:
            # Fetch records for the current batch
            batch_records = conn.execute(
                f"""
                SELECT * FROM {sanitize_table_name(org_table)}
                LIMIT {batch_size} OFFSET {offset}
                """
            ).fetchall()

            if not batch_records:
                return 0

            # Filter out records that already exist in mig_table
            filtered_records = [
                record for record in batch_records if record[0] not in existing_ids
            ]

            # Insert the filtered records into mig_table
            if filtered_records:
                insert_query = f"""
                INSERT INTO {sanitize_table_name(mig_table)} (customer_id, name, gender, DOB, Nationality)
                VALUES (?, ?, ?, ?, ?)
                """
                conn.executemany(insert_query, filtered_records)
                logger.info(
                    f"Thread {offset // batch_size}: Migrated {len(filtered_records)} records.")
                return len(filtered_records)

        return 0
    except Exception as e:
        logger.error(f"Error during batch migration (offset={offset}): {e}")
        raise


def verify_tables(db_path, table_name):
    """
    Verify that specified tables exist and log their record counts.

    Parameters:
        db_path (str): Path to the DuckDB database file.
        table_names (list): List of table names to verify.

    Raises:
        ValueError: If a table does not exist.
    """
    try:
        with duckdb.connect(db_path) as conn:
            # Check if the table exists
            result = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = ?", (
                    table_name,)
            ).fetchone()
            if not result:
                raise ValueError(f"Table '{table_name}' does not exist.")

            # Fetch record count
            count = conn.execute(
                # type: ignore
                f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(
                f"Table '{table_name}' exists with {count} records.")

            result2 = conn.execute(
                f"select * from {table_name} limit 10").fetchall()
            if not result2:
                raise ValueError(f"Table '{table_name}' does not exist.")
            logger.info(
                f"Table '{table_name}' first 10 records {result2}")

    except Exception as e:
        logger.error(f"Error verifying tables: {e}")
        raise


def fetch_table_row_counts(db_path):
    """
    Fetch all table names in the DuckDB database and return their record counts.

    Parameters:
        db_path (str): Path to the DuckDB database file.

    Returns:
        dict: A dictionary with table names as keys and their record counts as values.
    """
    table_counts = {}
    try:
        with duckdb.connect(db_path) as conn:
            # Fetch all table names
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';"
            ).fetchall()

            if not tables:
                logger.info("No tables found in the database.")
                return table_counts

            # Count rows for each table
            for table_name in tables:
                table_name = table_name[0]  # Extract table name from tuple
                count = conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                table_counts[table_name] = count
                logger.info(f"Table '{table_name}' has {count} records.")

        return table_counts
    except Exception as e:
        logger.error(f"Error fetching table row counts: {e}")
        raise


def drop_all_tables(db_path):
    """
    Drop all tables in the DuckDB database.

    Parameters:
        db_path (str): Path to the DuckDB database file.
    """
    try:
        with duckdb.connect(db_path) as conn:
            # Fetch all table names
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';"
            ).fetchall()

            for table_name in tables:
                table_name = table_name[0]  # Extract table name from tuple
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                logger.info(f"Dropped table: {table_name}")

            logger.info("All tables dropped successfully.")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")


def fetch_primary_keys(db_path, table_name):
    """
    Fetch all primary keys from a table.

    Parameters:
        db_path (str): Path to the DuckDB database file.
        table_name (str): Name of the table.

    Returns:
        set: A set of primary keys.
    """
    primary_keys = set()
    try:
        with duckdb.connect(db_path) as conn:
            primary_keys = {row[0] for row in conn.execute(
                f"SELECT customer_id FROM {table_name}").fetchall()}
        logger.info(
            f"Fetched {len(primary_keys)} primary keys from {table_name}.")
    except Exception as e:
        logger.error(f"Error fetching primary keys: {e}")
        raise
    return primary_keys


def process_and_insert_missing_records(db_path, source_table, target_table, existing_keys, batch_size=500):
    """
    Process records from source_table and insert missing records into target_table.

    Parameters:
        db_path (str): Path to the DuckDB database file.
        source_table (str): Table to fetch records from.
        target_table (str): Table to insert records into.
        existing_keys (set): Set of primary keys already in the target table.
        batch_size (int): Number of records to process in each batch.
    """
    try:
        with duckdb.connect(db_path) as conn:
            offset = 0
            while True:
                # Fetch records in batches
                batch = conn.execute(
                    f"SELECT * FROM {source_table} LIMIT {batch_size} OFFSET {offset}"
                ).fetchall()
                if not batch:
                    break

                # Filter records not in existing_keys
                missing_records = [
                    record for record in batch if record[0] not in existing_keys]

                # Insert missing records into target_table
                if missing_records:
                    insert_query = f"""
                    INSERT INTO {target_table} (customer_id, name, gender, DOB, Nationality)
                    VALUES (?, ?, ?, ?, ?)
                    """
                    conn.executemany(insert_query, missing_records)
                    logger.info(
                        f"Inserted {len(missing_records)} missing records into {target_table}.")

                # Update offset and existing_keys
                offset += batch_size
                existing_keys.update(record[0] for record in missing_records)

    except Exception as e:
        logger.error(f"Error processing records: {e}")
        raise
