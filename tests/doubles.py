class FakeCursor:
    """
    Minimal cursor double: records executed statements, plays back canned
    results. If fetchall_sequence is given, successive fetchall() calls pop
    from it in order (for code that reuses one cursor across multiple
    queries in a single `with` block); otherwise fetchall() always returns
    the fixed fetchall_return.
    """

    def __init__(self, fetchall_return=None, fetchone_return=None, lastrowid=None, fetchall_sequence=None):
        self.executed = []
        self._fetchall = fetchall_return if fetchall_return is not None else []
        self._fetchall_sequence = list(fetchall_sequence) if fetchall_sequence is not None else None
        self._fetchone = fetchone_return
        self.lastrowid = lastrowid
        self.rowcount = 1

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def executemany(self, sql, params_list):
        self.executed.append((sql, params_list))

    def fetchall(self):
        if self._fetchall_sequence is not None:
            return self._fetchall_sequence.pop(0)
        return self._fetchall

    def fetchone(self):
        return self._fetchone

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class FakeConn:
    """Minimal connection double: always hands back the same cursor."""

    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class FakeChangelog:
    def __init__(self):
        self.entries = []

    def write(self, op, table, **fields):
        self.entries.append({"op": op, "table": table, **fields})
