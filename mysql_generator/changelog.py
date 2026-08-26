import json
import threading
from datetime import datetime, timezone


class Changelog:
    """
    Append-only JSONL ground-truth log of every committed DDL/DML mutation the
    generator performs. Deliberately a local file, not a database table: a DB
    table would itself flow through the binlog as CDC noise that Artie/Openflow
    would need to be configured to exclude. Each entry is written only after
    its operation has actually committed, so the log never claims something
    happened that didn't.

    One line per committed operation, except bulk-seed inserts, which are
    logged once per batch (id range + count) rather than per row — logging
    millions of individual seed rows would defeat the point of batch inserts
    and bloat the file for a phase that isn't part of the CDC-mechanics signal
    under test.
    """

    def __init__(self, path):
        self._lock = threading.Lock()
        self._file = open(path, "a", buffering=1)

    def write(self, op, table, **fields):
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "op": op,
            "table": table,
        }
        entry.update(fields)
        line = json.dumps(entry, sort_keys=True)
        with self._lock:
            self._file.write(line + "\n")

    def close(self):
        with self._lock:
            self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def read_entries(path):
    """Read a changelog file back into a list of dicts (used by verification tooling/tests)."""
    entries = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return entries
