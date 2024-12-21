from sqlite3_db_operations import *

DB_PATH = "example.db"
TABLES_TO_VERIFY = ["customer", "customer_mig"]

if __name__ == "__main__":
    # try:
    #     print("Verifying tables...")
    #     verify_tables(DB_PATH, TABLES_TO_VERIFY)
    #     print("All tables verified successfully.")
    # except ValueError as e:
    #     print(e)
    # except Exception as e:
    #     print(f"Unexpected error: {e}")

    try:
        print("Fetching table row counts...")
        table_counts = fetch_table_row_counts(DB_PATH)
        if table_counts:
            print("Review of table row counts:")
            for table, count in table_counts.items():
                print(f"Table '{table}' has {count} records.")
        else:
            print("No tables found in the database.")
    except Exception as e:
        print(f"Unexpected error: {e}")
