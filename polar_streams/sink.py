from abc import ABC, abstractmethod
from time import sleep
import polars as pl


class Sink(ABC):
    def __init__(self, options: dict[str, str], df):
        self._options = options
        self._df = df

    @abstractmethod
    def save(self, path: None | str):
        raise NotImplementedError

    def process(self):
        for pl_df in self._df.process():
            self.write(pl_df)

    @abstractmethod
    def write(self, pl_df):
        raise NotImplementedError

    def control_loop(self):
        while True:
            self.process()
            sleep(self._options.get("processing_time", 1))


class ConsoleSink(Sink):
    _path = None

    def save(self, path: None | str):
        if not path:
            raise ValueError("Expected a path when calling save()")

        self._path = path
        self.process()

    def write(self, pl_df):
        print(pl_df.lazy().collect())


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
                ConsoleSink(self._options, self._df).save(path)
            case _:
                raise NotImplementedError(f"{self._format} is not implemented")