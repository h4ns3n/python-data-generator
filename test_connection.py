#!/usr/bin/env python3
import json
import argparse
import psycopg2

def parse_jdbc_url(jdbc_url):
    """
    Convert a JDBC URL of the form:
        jdbc:postgresql://host:port/dbname
    into a dictionary of connection parameters.
    """
    # Remove the "jdbc:" prefix if present.
    if jdbc_url.startswith("jdbc:"):
        jdbc_url = jdbc_url[len("jdbc:"):]
    # Ensure it starts with the expected prefix.
    if not jdbc_url.startswith("postgresql://"):
        raise ValueError("Invalid JDBC URL format.")
    jdbc_url = jdbc_url[len("postgresql://"):]
    try:
        host_port, dbname = jdbc_url.split("/", 1)
        host, port = host_port.split(":", 1)
        return {
            "host": host,
            "port": int(port),
            "dbname": dbname
        }
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
           "ssl": false
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
    # Use SSL mode if specified.
    db_params["sslmode"] = "require" if conn_info.get("ssl") else "disable"
    return db_params

def test_connection(db_params):
    """
    Test the database connection by connecting and querying the Postgres version.
    """
    try:
        connection = psycopg2.connect(**db_params)
        print("Successfully connected to the database!")
        with connection.cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            print("Database version:", version)
        connection.close()
    except Exception as e:
        print("Failed to connect to the database. Error:", e)

def main():
    parser = argparse.ArgumentParser(description="Test Connection to Postgres Data Source")
    parser.add_argument("--config", type=str, default="python-datasource.json",
                        help="Path to the database config JSON file.")
    parser.add_argument("--alias", type=str, default="PSQLDS1",
                        help="Configuration alias to use from the config file.")
    args = parser.parse_args()

    try:
        db_params = load_db_config(args.config, args.alias)
        test_connection(db_params)
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    main()