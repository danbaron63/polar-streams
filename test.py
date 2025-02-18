import time

import polars as pl

from polar_streams import polars

df = (
    polars.read_stream()
    .option("test", "test")
    .option("run_initial_batch", "false")
    .format("csv")
    .load("data")
)

df = (
    df.filter((pl.col("id") == 1) | (pl.col("id") == 2))
    .drop_duplicates(pl.col("id"))
    .with_columns(
        (pl.col("salary") * 1.2).alias("promotion"), pl.lit("COLUMN").alias("test")
    )
    .group_by("id")
    .agg(pl.sum("salary"))
)

query = (
    df.write_stream()
    .output_mode("complete")
    .option("checkpointLocation", "state-test")
    # .format("csv")
    # .save("out")
    .format("console")
    .save()
)
