from polar_streams import polars
import polars as pl


df = (
    polars
    .read_stream()
    .option("test","test")
    .option("run_initial_batch", "false")
    .format("csv")
    .load("data")
)

df = (
    df
    .filter((pl.col("id") == 1) | (pl.col("id") == 2))
    # .drop_duplicates(pl.col("id"))
    .with_columns(
        (pl.col("salary") * 1.2).alias("promotion"),
        pl.lit("COLUMN").alias("test")
    )
    .group_by("id")
    .agg(pl.sum("salary"))
)

(
    df
    .write_stream()
    # .format("csv")
    # .save("out")
    .format("console").save()
)
