import json


def parse_jdbc_url(jdbc_url):
    """
    Convert a JDBC URL of the form:
      jdbc:mysql://host:port/dbname
    into a dictionary of connection parameters.
    """
    if jdbc_url.startswith("jdbc:"):
        jdbc_url = jdbc_url[len("jdbc:"):]
    if not jdbc_url.startswith("mysql://"):
        raise ValueError("Invalid JDBC URL format.")
    jdbc_url = jdbc_url[len("mysql://"):]
    try:
        host_port, dbname = jdbc_url.split('/', 1)
        host, port = host_port.split(':')
        return {"host": host, "port": int(port), "database": dbname}
    except Exception as e:
        raise ValueError("Error parsing JDBC URL: " + str(e))


def load_db_config(config_file, alias):
    """
    Loads database configuration from a JSON file for the specified alias.
    Expected JSON structure:
    {
       "MYSQLDS1": {
           "url": "jdbc:mysql://host:port/dbname",
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
    db_params = parse_jdbc_url(conn_info["url"])
    db_params["user"] = conn_info.get("username", "")
    db_params["password"] = conn_info.get("password", "")
    if conn_info.get("ssl"):
        # PyMySQL treats a dict here as ssl.wrap_socket()-style kwargs (ca, cert,
        # key, ...); an empty dict enables TLS with the default context, mirroring
        # this repo's Postgres sslmode=require (encrypt, no cert verification).
        db_params["ssl"] = {}
    return db_params
