import polars as pl
import sqlite3
from pathlib import Path


class StateStore:
    def __init__(self, state_dir: str = "state"):
        self._state_dir = Path(state_dir)
        self._state_dir.mkdir(exist_ok=True, parents=True)
        self._path = self._state_dir / "state.db"
        self._uri = f"sqlite:///{state_dir}/state.db"
        self._con = sqlite3.connect(self._path)

    def write_state(self, pl_df: pl.LazyFrame, table_name: str):
        pl_df.collect().write_database(
            table_name=table_name,
            connection=self._uri,
            engine="adbc",
            if_table_exists="replace",
        )

    def state_exists(self, table_name: str) -> bool:
        cur = self._con.cursor()
        res = cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        return bool(res.fetchone())

    def get_state(self, table_name: str) -> pl.LazyFrame:
        return pl.read_database_uri(
            query=f"SELECT * FROM {table_name}",
            uri=self._uri,
            engine="adbc"
        ).lazy()

