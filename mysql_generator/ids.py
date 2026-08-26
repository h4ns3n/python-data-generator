import random
import threading
from collections import deque


class IdTracker:
    """
    Bounded in-memory cache of recently-seen row ids for one table, used to bias
    UPDATE/DELETE/transfer target selection toward "hot" recent rows the way a
    real OLTP workload tends to touch recent data far more than old data.
    Thread-safe: shared across worker threads in the concurrent simulate path.
    """

    def __init__(self, maxlen=5000):
        self._lock = threading.Lock()
        self._recent = deque(maxlen=maxlen)

    def record(self, row_id):
        with self._lock:
            self._recent.append(row_id)

    def discard(self, row_id):
        with self._lock:
            try:
                self._recent.remove(row_id)
            except ValueError:
                pass

    def sample_recent(self):
        with self._lock:
            if not self._recent:
                return None
            return random.choice(self._recent)

    def __len__(self):
        with self._lock:
            return len(self._recent)


def seed_from_table(conn, table_name, maxlen=5000):
    """
    Prime an IdTracker from a table's most recently created rows, so recency
    bias works immediately after the initial bulk-seed phase, not only for
    rows inserted during --simulate.
    """
    tracker = IdTracker(maxlen=maxlen)
    with conn.cursor() as cur:
        cur.execute(f"SELECT id FROM {table_name} ORDER BY id DESC LIMIT %s", (maxlen,))
        rows = cur.fetchall()
    for (row_id,) in reversed(rows):
        tracker.record(row_id)
    return tracker


def pick_target_id(conn, table_name, tracker, recent_bias=0.85):
    """
    Pick an existing row id to target for update/delete, biased toward recent
    rows. Returns None if the table appears to be empty.
    """
    if tracker is not None and len(tracker) > 0 and random.random() < recent_bias:
        candidate = tracker.sample_recent()
        if candidate is not None:
            return candidate

    with conn.cursor() as cur:
        cur.execute(f"SELECT MIN(id), MAX(id) FROM {table_name}")
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    lo, hi = row
    candidate = random.randint(lo, hi)

    with conn.cursor() as cur:
        cur.execute(f"SELECT id FROM {table_name} WHERE id >= %s ORDER BY id ASC LIMIT 1", (candidate,))
        row = cur.fetchone()
    return row[0] if row else None
