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

    def write_state(self, pl_df: pl.LazyFrame):
        pl_df.collect().write_database(
            table_name="state",
            connection=self._uri,
            engine="adbc",
            if_table_exists="replace",
        )

    def state_exists(self) -> bool:
        cur = self._con.cursor()
        res = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='state';")
        return bool(res.fetchone())

    def get_state(self) -> pl.LazyFrame:
        return pl.read_database_uri(
            query="SELECT * FROM state",
            uri=self._uri,
            engine="adbc"
        ).lazy()

