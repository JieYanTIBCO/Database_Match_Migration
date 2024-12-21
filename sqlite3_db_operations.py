from logging.handlers import RotatingFileHandler
import uuid
import sqlite3
import random
import string
from datetime import datetime, timedelta
import os
import logging

# Logging Configuration
logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[RotatingFileHandler('db_operation.log', maxBytes=5 * 1024 * 1024, backupCount=3),  # 5 MB per file, keep 3 backups
                              logging.StreamHandler()  # Log to the console
                              ])
logger = logging.getLogger(__name__)

# Constants
# DEFAULT_BATCH_SIZE = 500
# DEFAULT_MIGRATION_PERCENTAGE = 20


def sanitize_table_name(table_name):
    """
    Validate and sanitize table names to prevent SQL injection.
    """
    if not table_name.isidentifier():
        raise ValueError(f"Invalid table name: {table_name}")
    return table_name


def create_table(db_path, table_name, sql_file_path):
    """
    Create a table in the SQLite database based on a schema from a SQL file.

    Parameters:
        db_path (str): Path to the SQLite database file.
        table_name (str): Name of the table to create.
        sql_file_path (str): Path to the SQL file containing the table schema.
    """
    # Check if the SQL file exists
    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

    # Read the SQL schema from the file
    with open(sql_file_path, 'r') as file:
        create_table_ddl = file.read()

    # Replace placeholder with the table name
    create_table_ddl = create_table_ddl.replace(
        "{TABLE_NAME}", sanitize_table_name(table_name))

    # Create the table in the database
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(create_table_ddl)
            conn.commit()
            logger.info(f"Table '{table_name}' created successfully.")

            # Verify table existence
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
            if not cursor.fetchone():
                raise ValueError(
                    f"Failed to create table '{table_name}'. It does not exist.")
    except sqlite3.Error as e:
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
        # Generate random attributes
        name = ''.join(random.choices(
            string.ascii_letters, k=random.randint(5, 10)))
        gender = random.choice(['M', 'F', 'O'])
        DOB = datetime.strftime(
            datetime.now() - timedelta(days=random.randint(18 * 365, 70 * 365)), "%Y-%m-%d"
        )
        nationality = random.choice(['USA', 'Canada', 'China', 'India', 'UK'])

        # Use UUID for unique customer ID
        customer_id = str(uuid.uuid4())

        # Ensure uniqueness
        if customer_id not in unique_ids:
            unique_ids.add(customer_id)
            return (customer_id, name, gender, DOB, nationality)


def generate_mock_records(count):
    """
    Generate a list of mock records for the customer table.

    Parameters:
        count (int): Number of records to generate.

    Returns:
        list: A list of tuples representing records.
    """
    unique_ids = set()  # Initialize a set to track unique customer IDs
    return [generate_mock_record(unique_ids) for _ in range(count)]


def insert_records_in_batches(db_path, table_name, records, batch_size):
    """
    Insert records into a SQLite table in batches.

    Parameters:
        db_path (str): Path to the SQLite database file.
        table_name (str): Name of the table to insert records into.
        records (list): List of records to insert.
        batch_size (int): Number of records per batch.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            insert_query = f"""
            INSERT INTO {sanitize_table_name(table_name)} (customer_id, name, gender, DOB, Nationality)
            VALUES (?, ?, ?, ?, ?);
            """
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                cursor.executemany(insert_query, batch)
                conn.commit()
                logger.info(
                    f"Inserted batch {i // batch_size + 1} with {len(batch)} records.")
            logger.info("All records inserted successfully.")
    except sqlite3.Error as e:
        logger.error(f"Error inserting records into table '{table_name}': {e}")
        raise


def migrate_records_with_percentage_batch(db_path, org_table, mig_table, percentage, batch_size):
    """
    Migrate a percentage of records from org_table to mig_table in batches, avoiding duplicates.

    Parameters:
        db_path (str): Path to the SQLite database file.
        org_table (str): Name of the original table.
        mig_table (str): Name of the migration table.
        percentage (float): Percentage of records to migrate (e.g., 20 for 20%).
        batch_size (int): Number of records to process per batch.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Calculate the total number of records in org_table
            cursor.execute(
                f"SELECT COUNT(*) FROM {sanitize_table_name(org_table)}")
            total_records = cursor.fetchone()[0]

            # Calculate the number of records to migrate
            migrate_count = int(total_records * (percentage / 100.0))
            logger.info(f"Total records in {org_table}: {total_records}")
            logger.info(
                f"Migrating {migrate_count} records ({percentage}% of total).")

            # Fetch existing customer_ids in mig_table to avoid duplicates
            cursor.execute(
                f"SELECT customer_id FROM {sanitize_table_name(mig_table)}")
            existing_ids = set(row[0] for row in cursor.fetchall())

            # Initialize counters
            migrated_records = 0
            offset = 0

            while migrated_records < migrate_count:
                # Fetch a batch of records from org_table with offset
                limit = min(batch_size, migrate_count - migrated_records)
                cursor.execute(
                    f"SELECT * FROM {sanitize_table_name(org_table)} LIMIT {limit} OFFSET {offset}"
                )
                batch_records = cursor.fetchall()

                if not batch_records:
                    break

                # Filter out records that already exist in mig_table
                filtered_records = [
                    record for record in batch_records if record[0] not in existing_ids]

                # Insert filtered records into mig_table
                insert_query = f"""
                INSERT INTO {sanitize_table_name(mig_table)} (customer_id, name, gender, DOB, Nationality)
                VALUES (?, ?, ?, ?, ?)
                """
                cursor.executemany(insert_query, filtered_records)
                conn.commit()

                # Update counters
                migrated_count = len(filtered_records)
                migrated_records += migrated_count
                offset += limit
                logger.info(
                    f"Migrated {migrated_count} records in this batch. Total migrated: {migrated_records}")

            logger.info(
                f"Successfully migrated {migrated_records} records into {mig_table}.")
    except sqlite3.Error as e:
        logger.error(f"Error during migration: {e}")
        raise


def verify_tables(db_path, table_names):
    """
    Verify that specified tables exist and log their record counts.

    Parameters:
        db_path (str): Path to the SQLite database file.
        table_names (list): List of table names to verify.

    Raises:
        ValueError: If a table does not exist.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            for table_name in table_names:
                # Check if the table exists
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (
                        table_name,)
                )
                result = cursor.fetchone()
                if not result:
                    raise ValueError(f"Table '{table_name}' does not exist.")

                # Fetch record count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                logger.info(
                    f"Table '{table_name}' exists with {count} records.")
    except sqlite3.Error as e:
        logger.error(f"Error verifying tables: {e}")
        raise


def fetch_table_row_counts(db_path):
    """
    Fetch all table names in the database and return their record counts.

    Parameters:
        db_path (str): Path to the SQLite database file.

    Returns:
        dict: A dictionary with table names as keys and their record counts as values.
    """
    table_counts = {}
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Fetch all table names
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            if not tables:
                logger.info("No tables found in the database.")
                return table_counts

            # Count rows for each table
            for table_name in tables:
                table_name = table_name[0]  # Extract table name from tuple
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                table_counts[table_name] = count
                logger.info(f"Table '{table_name}' has {count} records.")

        return table_counts
    except sqlite3.Error as e:
        logger.error(f"Error fetching table row counts: {e}")
        raise


def drop_all_tables(db_path):
    """
    Drop all tables in the SQLite database.

    Parameters:
        db_path (str): Path to the SQLite database file.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Query all table names from sqlite_master
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            for table_name in tables:
                table_name = table_name[0]  # Extract table name from tuple
                cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                logger.info(f"Dropped table: {table_name}")

            conn.commit()
            logger.info("All tables dropped successfully.")
    except sqlite3.Error as e:
        logger.error(f"Error during cleanup: {e}")


def fetch_primary_keys(db_path, table_name):
    """
    Fetch all primary keys from a table.

    Parameters:
        db_path (str): Path to the SQLite database file.
        table_name (str): Name of the table.

    Returns:
        set: A set of primary keys.
    """
    primary_keys = set()
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT customer_id FROM {table_name}")
            primary_keys = {row[0] for row in cursor.fetchall()}
        logger.info(
            f"Fetched {len(primary_keys)} primary keys from {table_name}.")
    except sqlite3.Error as e:
        logger.error(f"Error fetching primary keys: {e}")
        raise
    return primary_keys


def process_and_insert_missing_records(db_path, source_table, target_table, existing_keys, batch_size=500):
    """
    Process records from source_table and insert missing records into target_table.

    Parameters:
        db_path (str): Path to the SQLite database file.
        source_table (str): Table to fetch records from.
        target_table (str): Table to insert records into.
        existing_keys (set): Set of primary keys already in the target table.
        batch_size (int): Number of records to process in each batch.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Fetch records from source_table in chunks
            offset = 0
            while True:
                cursor.execute(
                    f"SELECT * FROM {source_table} LIMIT {batch_size} OFFSET {offset}"
                )
                batch = cursor.fetchall()
                if not batch:
                    break

                # Filter records not in existing_keys
                missing_records = [
                    record for record in batch if record[0] not in existing_keys
                ]

                # Insert missing records into target_table
                if missing_records:
                    insert_query = f"""
                    INSERT INTO {target_table} (customer_id, name, gender, DOB, Nationality)
                    VALUES (?, ?, ?, ?, ?)
                    """
                    cursor.executemany(insert_query, missing_records)
                    conn.commit()
                    logger.info(
                        f"Inserted {len(missing_records)} missing records into {target_table}."
                    )

                # Update offset and existing_keys
                offset += batch_size
                existing_keys.update(record[0] for record in missing_records)

    except sqlite3.Error as e:
        logger.error(f"Error processing records: {e}")
        raise


def table_exists(db_path, table_name):
    """
    Check if a table exists in the SQLite database.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
                (table_name,)
            )
            return cursor.fetchone() is not None
    except sqlite3.Error as e:
        logger.error(f"Error checking table existence: {e}")
        return False
