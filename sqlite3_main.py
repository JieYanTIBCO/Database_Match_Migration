from sqlite3_db_operations import *

DB_PATH = "example.db"
SQL_FILE_PATH = "create_customer.sql"
ORG_TABLE = "customer"
MIG_TABLE = "customer_mig"
ORG_TABLE_SIZE = 100000  # Number of records in org_table
MIG_ADDITIONAL_RECORDS = 200000  # Additional records for mig_table
MIGRATION_PERCENTAGE = 100  # Migrate 50% of records
BATCH_SIZE = 2000  # Batch size for inserts


def main():
    """
    Main function to manage the full workflow:
    - Create tables.
    - Generate and insert data into org_table.
    - Create mig_table.
    - Migrate 50% of records from org_table to mig_table.
    - Generate and insert additional data into mig_table.
    - Clean up the old database first!!!!!
    """
    # Step 0: Clean up all the old tables
    drop_all_tables(DB_PATH)

    # Step 1: Create tables
    print("Creating tables...")
    create_table(DB_PATH, ORG_TABLE, SQL_FILE_PATH)
    create_table(DB_PATH, MIG_TABLE, SQL_FILE_PATH)

    # Step 2: Generate and insert data into org_table
    print(f"Generating {ORG_TABLE_SIZE} records for {ORG_TABLE}...")
    org_records = generate_mock_records(ORG_TABLE_SIZE)
    insert_records_in_batches(DB_PATH, ORG_TABLE, org_records, BATCH_SIZE)

    # Step 3: Migrate 50% of records from org_table to mig_table
    print(
        f"Migrating {MIGRATION_PERCENTAGE}% of records from {ORG_TABLE} to {MIG_TABLE}...")
    migrate_records_with_percentage_batch(
        DB_PATH, ORG_TABLE, MIG_TABLE, MIGRATION_PERCENTAGE, BATCH_SIZE)

    # Step 4: Generate and insert additional data into mig_table
    print(
        f"Generating {MIG_ADDITIONAL_RECORDS} additional records for {MIG_TABLE}...")
    mig_additional_records = generate_mock_records(MIG_ADDITIONAL_RECORDS)
    insert_records_in_batches(
        DB_PATH, MIG_TABLE, mig_additional_records, BATCH_SIZE)

    print("All steps completed successfully.")


if __name__ == "__main__":
    main()
