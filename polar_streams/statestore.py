import polars as pl
import sqlite3
from pathlib import Path
from contextlib import closing


class StateStore:
    def __init__(self, state_dir: str = "state"):
        self._state_dir = Path(state_dir)
        self._state_dir.mkdir(exist_ok=True, parents=True)
        self._path = self._state_dir / "state.db"
        self._uri = f"sqlite:///{state_dir}/state.db"
        self._con = sqlite3.connect(self._path, isolation_level=None)
        with closing(self._con.cursor()) as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS write_ahead_log (
                id INTEGER PRIMARY KEY,
                key VARCHAR
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS wal_commits (
                id INTEGER PRIMARY KEY,
                wal_id INTEGER
            );
            """)

    def write_state(self, pl_df: pl.LazyFrame, table_name: str) -> None:
        pl_df.collect().write_database(
            table_name=table_name,
            connection=self._uri,
            engine="adbc",
            if_table_exists="replace",
        )

    def state_exists(self, table_name: str) -> bool:
        with closing(self._con.cursor()) as cur:
            res = cur.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
            )
            return bool(res.fetchone())

    def get_state(self, table_name: str) -> pl.LazyFrame:
        return pl.read_database_uri(
            query=f"SELECT * FROM {table_name}", uri=self._uri, engine="adbc"
        ).lazy()

    def wal_append(self, key: str) -> int:
        with closing(self._con.cursor()) as cur:
            res = cur.execute(
                f"INSERT INTO write_ahead_log (key) VALUES ('{key}') RETURNING id;"
            )
            return int(res.fetchone()[0])

    def wal_commit(self, table: str, wal_id: int) -> None:
        with closing(self._con.cursor()) as cur:
            cur.execute(f"INSERT INTO wal_commits (wal_id) VALUES ({wal_id})")

    def wal_uncommitted_entries(self, table: str) -> list[str]:
        with closing(self._con.cursor()) as cur:
            res = cur.execute(f"SELECT MAX(wal_id) FROM wal_commits")
            max_wal_id = res.fetchone()[0]
            missing_entries = cur.execute(
                f"SELECT key FROM write_ahead_log WHERE id > {max_wal_id}"
            )
            return [k[0] for k in missing_entries.fetchall()]
