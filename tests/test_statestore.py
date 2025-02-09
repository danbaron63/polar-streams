from polar_streams.statestore import StateStore
import pytest
from tempfile import TemporaryDirectory
import polars as pl


@pytest.fixture(scope="session")
def state_dir():
    with TemporaryDirectory() as state_dir:
        yield state_dir


@pytest.fixture(scope="function")
def state_store(state_dir):
    yield StateStore(state_dir)


def test_not_state_exists(state_store):
    assert not state_store.state_exists("test")


def test_state_exists(state_store):
    # Given
    table_name = "test"
    cur = state_store._con.cursor()
    cur.execute(f"CREATE TABLE {table_name} (id INTEGER)")
    cur.close()

    # When
    exists = state_store.state_exists(table_name)

    # Then
    assert exists


def test_wal_append(state_store):
    # Given
    key_1 = "test-key-1"
    key_2 = "test-key-2"
    cur = state_store._con.cursor()

    # When
    id_1 = state_store.wal_append(key_1)
    id_2 = state_store.wal_append(key_2)

    # Then
    result = cur.execute("SELECT * FROM write_ahead_log ORDER BY id ASC;").fetchall()

    assert result == [
        (id_1, key_1),
        (id_2, key_2),
    ]
    assert id_1 == 1
    assert id_2 == 2


def test_wal_commit(state_store):
    # Given
    table = "test"
    id_1 = 1
    id_2 = 2
    cur = state_store._con.cursor()

    # When
    state_store.wal_commit(table, id_1)
    state_store.wal_commit(table, id_2)

    # Then
    result = cur.execute("SELECT * FROM wal_commits ORDER BY wal_id ASC;").fetchall()

    assert result == [
        (1, id_1),
        (2, id_2),
    ]


def test_wal_uncommited_entries(state_store):
    # Given
    table = "test"
    key_1 = "test-key-1"
    key_2 = "test-key-2"
    key_3 = "test-key-3"
    key_4 = "test-key-4"

    # When
    id_1 = state_store.wal_append(key_1)
    id_2 = state_store.wal_append(key_2)
    state_store.wal_append(key_3)
    state_store.wal_append(key_4)
    state_store.wal_commit(table, id_1)
    state_store.wal_commit(table, id_2)
    result = state_store.wal_uncommitted_entries(table)

    # Then
    assert result == [
        key_3,
        key_4,
    ]


def test_write_get_state(state_store):
    # Given
    df = pl.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    table_name = "test"
    cur = state_store._con.cursor()

    # When
    state_store.write_state(df.lazy(), table_name)
    result = state_store.get_state(table_name).collect()

    # Then
    df.equals(result)
    assert cur.execute(f"SELECT col1, col2 FROM {table_name};").fetchall() == [
        (1, 4),
        (2, 5),
        (3, 6),
    ]
