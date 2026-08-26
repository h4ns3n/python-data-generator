import threading
import time

from mysql_generator.concurrency import QuiesceGate


def test_dml_guard_allows_concurrent_dml():
    gate = QuiesceGate()
    active_at_once = []

    def worker():
        with gate.dml_guard():
            active_at_once.append(gate._active_dml)
            time.sleep(0.05)

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert max(active_at_once) > 1  # DML operations actually overlapped


def test_ddl_section_waits_for_in_flight_dml():
    gate = QuiesceGate()
    events = []

    def dml():
        with gate.dml_guard():
            events.append("dml_start")
            time.sleep(0.1)
            events.append("dml_end")

    def ddl():
        time.sleep(0.02)  # let the DML start first
        with gate.ddl_section():
            events.append("ddl_run")

    t_dml = threading.Thread(target=dml)
    t_ddl = threading.Thread(target=ddl)
    t_dml.start()
    t_ddl.start()
    t_dml.join()
    t_ddl.join()

    assert events == ["dml_start", "dml_end", "ddl_run"]


def test_ddl_section_blocks_new_dml_until_done():
    gate = QuiesceGate()
    events = []

    def slow_ddl():
        with gate.ddl_section():
            events.append("ddl_start")
            time.sleep(0.1)
            events.append("ddl_end")

    def dml():
        time.sleep(0.02)  # let the DDL section start first
        with gate.dml_guard():
            events.append("dml_run")

    t_ddl = threading.Thread(target=slow_ddl)
    t_dml = threading.Thread(target=dml)
    t_ddl.start()
    t_dml.start()
    t_ddl.join()
    t_dml.join()

    assert events == ["ddl_start", "ddl_end", "dml_run"]
