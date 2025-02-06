from polar_streams import polars
import polars as pl


df = (
    polars
    .read_stream()
    .option("test","test")
    .format("file")
    .load("data")
)

df = (
    df
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
    .format("console")
    .save("y")
)
