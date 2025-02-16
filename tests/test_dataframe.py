import logging
from tempfile import TemporaryDirectory

import polars as pl
from polars.testing import assert_frame_equal
from pytest import fixture

from polar_streams.dataframe import DataFrame
from polar_streams.model import Config, MicroBatch
from polar_streams.statestore import StateStore

logger = logging.getLogger(__name__)


class MockDataFrame(DataFrame):
    def __init__(self, microbatches: list[MicroBatch]):
        super().__init__(None)
        self._microbatches = microbatches

    def process(self, state_store: StateStore, config: Config):
        for microbatch in self._microbatches:
            logging.info(
                f"yielding microbatch {microbatch}, {microbatch.pl_df.collect()}"
            )
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


@fixture
def state_store():
    with TemporaryDirectory() as state_dir:
        yield StateStore(state_dir)


@fixture
def duplicate_df():
    df_1 = pl.DataFrame({"id": [1, 2, 2], "col2": [4, 5, 5]}).lazy()
    df_2 = pl.DataFrame({"id": [2, 8, 9], "col2": [5, 11, 12]}).lazy()
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


def test_group_by(duplicate_df, state_store):
    result_df = duplicate_df.group_by("id").agg(pl.col("col2").sum())

    dfs = [
        mb.pl_df.collect()
        for mb in result_df.process(state_store=state_store, config=None)
    ]
    assert_frame_equal(
        dfs[0], pl.DataFrame({"id": [1, 2], "col2": [4, 10]}), check_row_order=False
    )
    assert_frame_equal(
        dfs[1],
        pl.DataFrame({"id": [1, 2, 8, 9], "col2": [4, 15, 11, 12]}),
        check_row_order=False,
    )


def test_filter(source_df):
    result_df = source_df.filter(pl.col("col1") != 2)

    dfs = [mb.pl_df.collect() for mb in result_df.process(None, None)]
    assert_frame_equal(dfs[0], pl.DataFrame({"col1": [1, 3], "col2": [4, 6]}))
    assert_frame_equal(dfs[1], pl.DataFrame({"col1": [7, 8, 9], "col2": [10, 11, 12]}))


def test_drop_duplicates(duplicate_df, state_store):
    result_df = duplicate_df.drop_duplicates("id")

    dfs = [
        mb.pl_df.collect()
        for mb in result_df.process(state_store=state_store, config=None)
    ]
    assert_frame_equal(
        dfs[0], pl.DataFrame({"id": [1, 2], "col2": [4, 5]}), check_row_order=False
    )
    assert_frame_equal(
        dfs[1], pl.DataFrame({"id": [8, 9], "col2": [11, 12]}), check_row_order=False
    )
