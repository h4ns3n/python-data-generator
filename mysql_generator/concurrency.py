import threading


class QuiesceGate:
    """
    Coordinates DDL drift events against concurrent DML workers. A single
    global gate (DML = readers, DDL = writer) rather than a per-table lock:
    migrations are rare (~minutes apart) and each pause is a single-statement
    commit, so a brief global pause is cheap and far simpler to reason about
    than per-table reader/writer locks.

    DML workers wrap each operation in dml_guard(); when a DDL event needs to
    fire, it wraps the ALTER in ddl_section(), which blocks new DML from
    starting and waits for in-flight DML to finish before proceeding.
    """

    def __init__(self):
        self._cond = threading.Condition()
        self._ddl_pending = False
        self._active_dml = 0

    def dml_guard(self):
        return _DmlGuard(self)

    def ddl_section(self):
        return _DdlSection(self)


class _DmlGuard:
    def __init__(self, gate):
        self._gate = gate

    def __enter__(self):
        with self._gate._cond:
            while self._gate._ddl_pending:
                self._gate._cond.wait()
            self._gate._active_dml += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._gate._cond:
            self._gate._active_dml -= 1
            if self._gate._active_dml == 0:
                self._gate._cond.notify_all()
        return False


class _DdlSection:
    def __init__(self, gate):
        self._gate = gate

    def __enter__(self):
        with self._gate._cond:
            self._gate._ddl_pending = True
            while self._gate._active_dml > 0:
                self._gate._cond.wait()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._gate._cond:
            self._gate._ddl_pending = False
            self._gate._cond.notify_all()
        return False
