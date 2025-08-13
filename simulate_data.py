#!/usr/bin/env python3
import argparse
import json
import random
import string
import time
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from psycopg2 import pool
import concurrent.futures

def parse_jdbc_url(jdbc_url):
    """
    Convert a JDBC URL of the form:
      jdbc:postgresql://host:port/dbname
    into a dictionary of connection parameters.
    """
    # Remove the "jdbc:" prefix.
    if jdbc_url.startswith("jdbc:"):
        jdbc_url = jdbc_url[len("jdbc:"):]
    # Now we expect: "postgresql://host:port/dbname"
    if not jdbc_url.startswith("postgresql://"):
        raise ValueError("Invalid JDBC URL format.")
    jdbc_url = jdbc_url[len("postgresql://"):]
    try:
        # Split into host:port and dbname.
        host_port, dbname = jdbc_url.split('/', 1)
        host, port = host_port.split(':')
        port = int(port)
        return {"host": host, "port": port, "dbname": dbname}
    except Exception as e:
        raise ValueError("Error parsing JDBC URL: " + str(e))

def load_db_config(config_file, alias):
    """
    Loads database configuration from a JSON file for the specified alias.
    Expected JSON structure:
    {
       "PSQLDS1": {
           "url": "jdbc:postgresql://host:port/dbname",
           "username": "myuser",
           "password": "mypassword",
           "ssl": true
       }
    }
    """
    with open(config_file, "r") as f:
        config_data = json.load(f)
    if alias not in config_data:
        raise ValueError(f"Alias '{alias}' not found in config file.")

    conn_info = config_data[alias]
    # Parse the JDBC URL to extract host, port, and database name.
    db_params = parse_jdbc_url(conn_info["url"])
    db_params["user"] = conn_info.get("username", "")
    db_params["password"] = conn_info.get("password", "")
    
    # Option: Use the ssl flag from our config.
    # Here "require" means the client will enforce an SSL connection.
    # If you want to enforce it you can change this to "require".
    db_params["sslmode"] = "require" if conn_info.get("ssl") else "disable"

    return db_params

def create_table_if_not_exists(conn, table_name):
    """
    Creates a table (if not exists) with a sample transactions schema:
        - id: a serial primary key,
        - transaction_ts: timestamp (defaults to current timestamp),
        - amount: a numeric field,
        - description: a text field.
    """
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        transaction_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        amount NUMERIC(10,2) NOT NULL,
        description TEXT
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_sql)
    conn.commit()

def reset_table(conn, table_name):
    """
    Drops the table if exists and then creates the table with the transactions schema.
    """
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    conn.commit()
    create_table_if_not_exists(conn, table_name)

def insert_records(conn, table_name, num_records, batch_size=10000):
    """
    Sequentially insert num_records into the specified table.
    Uses a progress indicator (tqdm) if available.
    """
    try:
        from tqdm import tqdm
        progress = tqdm(total=num_records, desc=f"Inserting records into {table_name}")
    except ImportError:
        progress = None

    insert_sql = f"INSERT INTO {table_name} (amount, description) VALUES %s"
    with conn.cursor() as cur:
        records = []
        for i in range(1, num_records + 1):
            # Generate a random transaction amount between 1.00 and 1000.00.
            amount = round(random.uniform(1.0, 1000.0), 2)
            # Generate a random alphanumeric description of length 10.
            description = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
            records.append((amount, description))
            
            # Once the batch size is reached, insert records and commit.
            if i % batch_size == 0:
                execute_values(cur, insert_sql, records)
                conn.commit()
                if progress:
                    progress.update(batch_size)
                records = []  # reset for the next batch
        
        # Insert any remaining records.
        if records:
            execute_values(cur, insert_sql, records)
            conn.commit()
            if progress:
                progress.update(len(records))
    
    if progress:
        progress.close()

def insert_batch_pool(connection_pool, table_name, batch):
    """
    Helper function that obtains a connection from the pool and inserts a batch of records.
    """
    insert_sql = f"INSERT INTO {table_name} (amount, description) VALUES %s"
    conn = None
    try:
        conn = connection_pool.getconn()
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, batch)
        conn.commit()
    except Exception as e:
        print(f"Error inserting batch for {table_name}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            connection_pool.putconn(conn)

def generate_batches(num_records, batch_size):
    """
    Generator function to yield one batch of records at a time
    without pre-building the entire list.
    """
    batch = []
    for i in range(1, num_records + 1):
        amount = round(random.uniform(1.0, 1000.0), 2)
        description = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        batch.append((amount, description))
        if i % batch_size == 0:
            yield batch
            batch = []
    if batch:
        yield batch

def insert_records_concurrent(db_params, table_name, num_records, batch_size=10000, num_workers=4):
    """
    Concurrently insert num_records into the specified table using a thread pool
    with a connection pool to reduce connection creation overhead.
    """
    try:
        from tqdm import tqdm
        progress = tqdm(total=(num_records + batch_size - 1) // batch_size,
                        desc=f"Concurrent inserts into {table_name}")
    except ImportError:
        progress = None

    # Create a connection pool with a maximum of num_workers connections.
    connection_pool = psycopg2.pool.SimpleConnectionPool(1, num_workers, **db_params)

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for batch in generate_batches(num_records, batch_size):
            futures.append(executor.submit(insert_batch_pool, connection_pool, table_name, batch))
            if progress:
                progress.update(1)
        for future in concurrent.futures.as_completed(futures):
            future.result()  # re-raise exception if any.
    if progress:
        progress.close()
    connection_pool.closeall()

def simulate_production(conn, table_names, batch_size, delay):
    """
    Simulate a production environment that continuously inserts new records
    into randomly chosen tables. Each batch represents new 'transactions'.
    """
    print("Starting production simulation. Press Ctrl+C to stop.")
    try:
        while True:
            table_name = random.choice(table_names)
            records = []
            for _ in range(batch_size):
                amount = round(random.uniform(1.0, 1000.0), 2)
                description = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
                records.append((amount, description))
            insert_sql = f"INSERT INTO {table_name} (amount, description) VALUES %s"
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, records)
            conn.commit()
            print(f"Inserted batch of {batch_size} records into {table_name} at {datetime.now()}")
            time.sleep(delay)
    except KeyboardInterrupt:
        print("Simulation stopped by user.")

def main():
    parser = argparse.ArgumentParser(description="Postgres Sample Data Generator")
    parser.add_argument("--config", type=str, default="python-datasource.json",
                        help="Path to the DB config JSON file.")
    parser.add_argument("--alias", type=str, default="PSQLDS1",
                        help="Configuration alias to use from the config file.")
    parser.add_argument("--num_tables", type=int, default=1,
                        help="Total number of tables to consider (1 to 1000).")
    parser.add_argument("--start_table", type=int, default=1,
                        help="Starting table number to process. Tables with lower numbers will be skipped.")
    parser.add_argument("--records_per_table", type=int, default=1000,
                        help="Number of records to insert per table (1000 to 10000000).")
    parser.add_argument("--simulate", action="store_true",
                        help="Enable production simulation mode (continuous inserts).")
    parser.add_argument("--batch_size", type=int, default=10000,
                        help="Batch size used for inserts.")
    parser.add_argument("--delay", type=float, default=1.0,
                        help="Delay (in seconds) between simulation batches.")
    parser.add_argument("--num_workers", type=int, default=1,
                        help="Number of concurrent workers for record insertion. Defaults to 1 (sequential).")
    parser.add_argument("--reset_all", action="store_true",
                        help="If set, drop and recreate all processed tables.")
    parser.add_argument("--reset_tables", type=str, default="",
                        help="Comma-separated list of table numbers to reset (i.e. drop and recreate). E.g. '1,3,5'.")
    args = parser.parse_args()

    # Validate command-line arguments.
    if not (1 <= args.num_tables <= 1000):
        raise ValueError("num_tables must be between 1 and 1000.")
    if not (1000 <= args.records_per_table <= 10000000):
        raise ValueError("records_per_table must be between 1000 and 10000000.")
    if args.start_table < 1:
        raise ValueError("start_table must be 1 or greater.")
    if args.start_table > args.num_tables:
        print("start_table is greater than num_tables. Nothing to process.")
        return

    # Process the list of tables to reset if provided
    reset_tables_set = set()
    if args.reset_tables:
        try:
            reset_tables_set = set(int(x.strip()) for x in args.reset_tables.split(',') if x.strip() != "")
        except Exception as e:
            print("Error processing --reset_tables:", e)
            return

    # Load database configuration.
    try:
        db_params = load_db_config(args.config, args.alias)
    except Exception as e:
        print("Error loading config:", e)
        return

    # Connect to the Postgres database.
    try:
        conn = psycopg2.connect(**db_params)
    except Exception as e:
        print("Error connecting to the database:", e)
        return

    # Create the specified number of tables and insert the requested record count into each.
    table_names = []
    for i in range(args.start_table, args.num_tables + 1):
        table_name = f"transactions_{i}"
        table_names.append(table_name)
        if args.reset_all or (i in reset_tables_set):
            # For tables starting from the specified start_table, drop existing table and recreate.
            print(f"Resetting table: {table_name}")
            reset_table(conn, table_name)
        else:
            # For older tables, just create if they do not exist (appending new data).
            print(f"Creating table (if not exists): {table_name}")
            create_table_if_not_exists(conn, table_name)

        print(f"Inserting {args.records_per_table} records into {table_name}.")
        if args.num_workers > 1:
            # Use concurrent insertion
            insert_records_concurrent(db_params, table_name, args.records_per_table, batch_size=args.batch_size, num_workers=args.num_workers)
        else:
            insert_records(conn, table_name, args.records_per_table, batch_size=args.batch_size)

    # If simulation mode is enabled, continuously insert batches of new records.
    if args.simulate:
        simulate_production(conn, table_names, args.batch_size, args.delay)

    # Close the database connection.
    conn.close()

if __name__ == "__main__":
    main() 