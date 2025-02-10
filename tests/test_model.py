# mypy: disable-error-code="no-untyped-def"
from polar_streams.model import MicroBatch, Metadata
from datetime import datetime
import polars as pl


def test_microbatch_new():
    # Given
    df = pl.DataFrame({"col1": [1, 2, 3]}).lazy()
    start_time = datetime.now()
    metadata = Metadata(start_time=start_time, source_files=[], wal_ids=[])
    microbatch = MicroBatch(pl_df=df, metadata=metadata)

    # When
    new_microbatch = microbatch.new(df)

    # Then
    assert id(microbatch) != id(new_microbatch)
    assert id(microbatch.metadata) == id(new_microbatch.metadata)
    assert id(microbatch.pl_df) == id(new_microbatch.pl_df)
