import threading

from mysql_generator.changelog import Changelog, read_entries


def test_write_then_read_round_trip(tmp_path):
    path = tmp_path / "changelog.jsonl"
    with Changelog(str(path)) as cl:
        cl.write("insert", "transactions_1", id=1, amount="10.00")
        cl.write("ddl", "transactions_1", ddl_type="add_column", column="extra_x")

    entries = read_entries(str(path))
    assert len(entries) == 2
    assert entries[0]["op"] == "insert"
    assert entries[0]["table"] == "transactions_1"
    assert entries[0]["id"] == 1
    assert entries[1]["ddl_type"] == "add_column"
    assert "ts" in entries[0]


def test_each_entry_is_one_line(tmp_path):
    path = tmp_path / "changelog.jsonl"
    with Changelog(str(path)) as cl:
        for i in range(5):
            cl.write("insert", "transactions_1", id=i)
    lines = path.read_text().strip().split("\n")
    assert len(lines) == 5


def test_concurrent_writes_do_not_corrupt_lines(tmp_path):
    path = tmp_path / "changelog.jsonl"
    cl = Changelog(str(path))

    def writer(n):
        for i in range(50):
            cl.write("insert", "transactions_1", id=n * 1000 + i)

    threads = [threading.Thread(target=writer, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    cl.close()

    entries = read_entries(str(path))
    assert len(entries) == 8 * 50
    assert len({e["id"] for e in entries}) == 8 * 50
