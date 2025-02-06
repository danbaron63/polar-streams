from polar_streams import polars


df = (
    polars
    .read_stream()
    .option("test","test")
    .format("file")
    .load("data")
)

print(df.__dict__)

(
    df
    .write_stream()
    .format("console")
    .save("y")
)
