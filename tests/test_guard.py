import pytest

from mysql_generator.guard import ReadOnlyEndpointError, assert_writable
from tests.doubles import FakeConn, FakeCursor


def test_assert_writable_passes_on_writer():
    conn = FakeConn(FakeCursor(fetchone_return=(0,)))
    assert_writable(conn)  # should not raise


def test_assert_writable_raises_on_reader():
    conn = FakeConn(FakeCursor(fetchone_return=(1,)))
    with pytest.raises(ReadOnlyEndpointError):
        assert_writable(conn)
