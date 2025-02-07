import polars as pl
import sqlite3

class StateStore:
    _uri = "sqlite:///out/state.db"

    def write_state(self, pl_df: pl.DataFrame):
        pl_df.collect().write_database(
            table_name="state",
            connection=self._uri,
            engine="adbc",
            if_table_exists="replace",
        )

    def state_exists(self) -> bool:
        con = sqlite3.connect("out/state.db")
        cur = con.cursor()
        res = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='state';")
        return bool(res.fetchone())

    def get_state(self) -> pl.LazyFrame:
        return pl.read_database_uri(
            query="SELECT * FROM state",
            uri=self._uri,
            engine="adbc"
        ).lazy()

