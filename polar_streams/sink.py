from abc import ABC, abstractmethod
from time import sleep
import polars as pl
from uuid import uuid1
from pathlib import Path


class Sink(ABC):
    def __init__(self, options: dict[str, str], df):
        self._options = options
        self._df = df
        self._path = None

    def save(self) :
        for pl_df in self._df.process():
            self.write(pl_df)

    @abstractmethod
    def write(self, pl_df):
        raise NotImplementedError


class ConsoleSink(Sink):
    def write(self, pl_df):
        print(pl_df.lazy().collect())


class FileSink(Sink):
    def __init__(self, options: dict[str, str], df, fmt: str, path: Path):
        super().__init__(options, df)
        self._path = path
        self._path.mkdir(parents=True, exist_ok=True)
        self._format = fmt

    def write(self, pl_df: pl.LazyFrame):
        # TODO: Consider using a monotonic counter if ordering of files is important
        path = self._path / f"{uuid1()}.{self._format}"
        match self._format:
            case "csv":
                pl_df.sink_csv(path)
            case "parquet":
                pl_df.sink_parquet(path)
            case "json":
                pl_df.sink_ndjson(path)
            case _:
                raise ValueError(f"{self._format} is not supported")


class SinkFactory:
    def __init__(self, df):
        self._df = df
        self._options = dict()
        self._format = None

    def option(self, key, value) -> "SinkFactory":
        self._options[key] = value
        return self

    def format(self, fmt) -> "SinkFactory":
        self._format = fmt
        return self

    def save(self, path: None | str = None):
        match self._format:
            case "console":
                sink = ConsoleSink(self._options, self._df)
            case "csv" | "parquet" | "json":
                sink = FileSink(self._options, self._df, self._format, Path(path))
            case _:
                raise ValueError(f"{self._format} is not supported")
        sink.save()
