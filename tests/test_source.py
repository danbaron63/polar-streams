from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from pytest import fixture

from polar_streams.model import Config, OutputMode
from polar_streams.source import FileSource
from polar_streams.statestore import StateStore


@fixture
def csv_source():
    source = FileSource(
        options=dict(),
        fmt="csv",
    )
    return source


def test_path_required(csv_source):
    with pytest.raises(ValueError) as exc_info:
        csv_source.load(None)

    assert str(exc_info.value) == "Expected a path when calling load()"


def test_path_set(csv_source):
    df = csv_source.load("test-path")

    assert df._source._path.as_posix() == "test-path"


def test_batch_source(csv_source):
    with TemporaryDirectory() as temp_dir:
        with TemporaryDirectory() as state_dir:
            csv_source._path = Path(temp_dir)
            config = Config(dict(), OutputMode.COMPLETE)
            df1 = pl.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
            df2 = pl.DataFrame({"col1": [7, 8, 9], "col2": [10, 11, 12]})

            df1.write_csv(Path(temp_dir) / "source-1.csv")
            df2.write_csv(Path(temp_dir) / "source-2.csv")

            out_df = next(csv_source.process(StateStore(state_dir), config))

            assert_frame_equal(
                out_df.pl_df.collect(),
                pl.DataFrame(
                    {
                        "col1": [1, 2, 3, 7, 8, 9],
                        "col2": [4, 5, 6, 10, 11, 12],
                    }
                ),
            )
