import queue
import pymysql


class SimpleConnectionPool:
    """
    Minimal fixed-size connection pool. pymysql has no built-in pooling
    (unlike psycopg2.pool), so this hands out pre-opened connections from
    a queue and returns them on release.
    """
    def __init__(self, db_params, size):
        self._queue = queue.Queue(maxsize=size)
        for _ in range(size):
            self._queue.put(pymysql.connect(**db_params))

    def getconn(self):
        return self._queue.get()

    def putconn(self, conn):
        self._queue.put(conn)

    def closeall(self):
        while not self._queue.empty():
            self._queue.get().close()
