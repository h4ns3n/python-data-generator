from mysql_generator import pool as pool_module


class FakeMySQLConnection:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.closed = False

    def close(self):
        self.closed = True


def test_pool_creates_size_connections_and_reuses_them(monkeypatch):
    created = []

    def fake_connect(**kwargs):
        conn = FakeMySQLConnection(**kwargs)
        created.append(conn)
        return conn

    monkeypatch.setattr(pool_module.pymysql, "connect", fake_connect)

    p = pool_module.SimpleConnectionPool({"host": "h"}, size=3)
    assert len(created) == 3

    conn = p.getconn()
    assert conn in created
    p.putconn(conn)

    # pool should still only ever have 3 distinct connections in circulation
    seen = {p.getconn() for _ in range(3)}
    assert seen == set(created)


def test_pool_closeall_closes_every_connection(monkeypatch):
    def fake_connect(**kwargs):
        return FakeMySQLConnection(**kwargs)

    monkeypatch.setattr(pool_module.pymysql, "connect", fake_connect)

    p = pool_module.SimpleConnectionPool({"host": "h"}, size=2)
    conns = [p.getconn(), p.getconn()]
    for c in conns:
        p.putconn(c)

    p.closeall()
    assert all(c.closed for c in conns)
