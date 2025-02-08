from abc import ABC, abstractmethod
import polars as pl
from uuid import uuid1
from pathlib import Path
from multiprocessing import Process
from polar_streams.config import OutputMode, Config


class Sink(ABC):
    def __init__(self, config: Config, df):
        self._config = config
        self._df = df
        self._path = None
        self._df.set_config(self._config)

    def save(self) -> "QueryManager":
        def pull_loop():
            for pl_df in self._df.process():
                self.write(pl_df)

        p = Process(target=pull_loop)
        p.start()
        return QueryManager(p)

    @abstractmethod
    def write(self, pl_df):
        raise NotImplementedError


class ConsoleSink(Sink):
    def write(self, pl_df):
        print(pl_df.lazy().collect())


class FileSink(Sink):
    def __init__(self, config: Config, df, fmt: str, path: Path):
        super().__init__(config, df)
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
        self._output_mode = None

    def option(self, key, value) -> "SinkFactory":
        self._options[key] = value
        return self

    def format(self, fmt) -> "SinkFactory":
        self._format = fmt
        return self

    def output_mode(self, mode: str):
        self._output_mode = OutputMode(mode)
        return self

    @property
    def _config(self) -> Config:
        return Config(
            write_options=self._options,
            output_mode=self._output_mode
        )

    def save(self, path: None | str = None) -> "QueryManager":
        match self._format:
            case "console":
                sink = ConsoleSink(self._config, self._df)
            case "csv" | "parquet" | "json":
                sink = FileSink(self._config, self._df, self._format, Path(path))
            case _:
                raise ValueError(f"{self._format} is not supported")
        return sink.save()


class QueryManager:
    def __init__(self, query_process: Process):
        self._query_process = query_process

    def stop(self):
        self._query_process.terminate()
        self._query_process.join()