import shutil
import subprocess

import pytest


def _docker_available():
    if shutil.which("docker") is None:
        return False
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=5, check=True)
        return True
    except Exception:
        return False


collect_ignore_glob = []
if not _docker_available():
    collect_ignore_glob.append("test_*.py")


@pytest.fixture(scope="module")
def mysql_container():
    testcontainers_mysql = pytest.importorskip("testcontainers.mysql")
    with testcontainers_mysql.MySqlContainer("mysql:8.0") as container:
        yield container


@pytest.fixture
def db_params(mysql_container):
    return {
        "host": mysql_container.get_container_host_ip(),
        "port": int(mysql_container.get_exposed_port(3306)),
        "user": mysql_container.username,
        "password": mysql_container.password,
        "database": mysql_container.dbname,
    }
