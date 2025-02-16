import polars as pl
from polars.testing import assert_frame_equal
from pytest import fixture

from polar_streams.dataframe import DataFrame
from polar_streams.model import Config, MicroBatch
from polar_streams.statestore import StateStore


class MockDataFrame(DataFrame):
    def __init__(self, microbatches: list[MicroBatch]):
        super().__init__(None)
        self._microbatches = microbatches

    def process(self, state_store: StateStore, config: Config):
        for microbatch in self._microbatches:
            print(f"yielding microbatch {microbatch}, {microbatch.pl_df.collect()}")
            yield microbatch


@fixture
def source_df():
    df_1 = pl.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]}).lazy()
    df_2 = pl.DataFrame({"col1": [7, 8, 9], "col2": [10, 11, 12]}).lazy()
    source_df = MockDataFrame(
        [
            MicroBatch(pl_df=df_1, metadata=None),
            MicroBatch(pl_df=df_2, metadata=None),
        ]
    )
    return DataFrame(source_df)


def test_with_columns(source_df):
    result_df = source_df.with_columns((pl.col("col1") * 3).alias("col3"))

    dfs = [mb.pl_df.collect() for mb in result_df.process(None, None)]
    assert_frame_equal(
        dfs[0], pl.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6], "col3": [3, 6, 9]})
    )
    assert_frame_equal(
        dfs[1],
        pl.DataFrame({"col1": [7, 8, 9], "col2": [10, 11, 12], "col3": [21, 24, 27]}),
    )


def test_with_column(source_df):
    result_df = source_df.with_column(pl.lit("test").alias("col3"))

    dfs = [mb.pl_df.collect() for mb in result_df.process(None, None)]
    assert_frame_equal(
        dfs[0],
        pl.DataFrame(
            {"col1": [1, 2, 3], "col2": [4, 5, 6], "col3": ["test", "test", "test"]}
        ),
    )
    assert_frame_equal(
        dfs[1],
        pl.DataFrame(
            {"col1": [7, 8, 9], "col2": [10, 11, 12], "col3": ["test", "test", "test"]}
        ),
    )


def test_select(source_df):
    result_df = source_df.select("col1", (pl.col("col1") + 1).alias("col3"))

    dfs = [mb.pl_df.collect() for mb in result_df.process(None, None)]
    assert_frame_equal(dfs[0], pl.DataFrame({"col1": [1, 2, 3], "col3": [2, 3, 4]}))
    assert_frame_equal(dfs[1], pl.DataFrame({"col1": [7, 8, 9], "col3": [8, 9, 10]}))
