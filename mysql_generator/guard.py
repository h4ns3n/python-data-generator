class ReadOnlyEndpointError(RuntimeError):
    """Raised when the target endpoint cannot accept writes (e.g. an Aurora reader)."""


def assert_writable(conn):
    """
    Aurora MySQL only generates binary logs on the writer instance, and reader
    endpoints reject DML outright. Fail fast with a clear message instead of
    letting the first INSERT/DDL statement surface a confusing driver error.

    Note: on Aurora, a reader instance reports @@read_only=0 (that variable
    reflects standalone-MySQL replication state, which Aurora's shared-storage
    architecture doesn't use) — the actual signal is @@innodb_read_only=1.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT @@innodb_read_only;")
        (read_only,) = cur.fetchone()
    if read_only:
        raise ReadOnlyEndpointError(
            "Connected host has innodb_read_only=ON — this looks like the reader "
            "endpoint. CDC/DDL/DML generation must target the writer host (see "
            "writer_host in the connection config); the reader has no binlog of its own."
        )
