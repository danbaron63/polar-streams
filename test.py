from polar_streams import polars
import polars as pl




df = (
    polars
    .read_stream()
    .option("test","test")
    .format("csv")
    .load("data")
)

df = (
    df
    .filter((pl.col("id") == 1) | (pl.col("id") == 2))
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
