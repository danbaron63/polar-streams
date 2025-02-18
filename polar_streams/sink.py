from abc import ABC, abstractmethod
from multiprocessing import Process
from pathlib import Path
from uuid import uuid1

from polar_streams.model import Config, MicroBatch, OutputMode
from polar_streams.statestore import StateStore
from polar_streams.util import log


class Sink(ABC):
    def __init__(self, config: Config, df) -> None:
        self._config = config
        self._df = df
        self._state_store: StateStore = StateStore(
            self._config.write_options["checkpointLocation"]
        )

    @log()
    def save(self) -> "QueryManager":
        def pull_loop() -> None:
            for microbatch in self._df.process(self._state_store, self._config):
                self.write(microbatch)

        p = Process(target=pull_loop)
        p.start()
        return QueryManager(p)

    @abstractmethod
    def write(self, microbatch: MicroBatch):
        raise NotImplementedError


class ConsoleSink(Sink):
    @log()
    def write(self, microbatch: MicroBatch):
        print(microbatch.pl_df.lazy().collect())
        for wal_id in microbatch.metadata.wal_ids:
            self._state_store.wal_commit(wal_id)


class FileSink(Sink):
    def __init__(self, config: Config, df, fmt: str, path: Path):
        super().__init__(config, df)
        self._path = path
        self._path.mkdir(parents=True, exist_ok=True)
        self._format = fmt

    @log()
    def write(self, microbatch: MicroBatch):
        # TODO: Consider using a monotonic counter if ordering of files is important
        path = self._path / f"{uuid1()}.{self._format}"
        match self._format:
            case "csv":
                microbatch.pl_df.sink_csv(path)
            case "parquet":
                microbatch.pl_df.sink_parquet(path)
            case "json":
                microbatch.pl_df.sink_ndjson(path)
            case _:
                raise ValueError(f"{self._format} is not supported")


class SinkFactory:
    def __init__(self, df):
        self._df = df
        self._options = dict()
        self._format = None
        self._output_mode: OutputMode = OutputMode.APPEND

    @log()
    def option(self, key, value) -> "SinkFactory":
        self._options[key] = value
        return self

    @log()
    def format(self, fmt) -> "SinkFactory":
        self._format = fmt
        return self

    @log()
    def output_mode(self, mode: str):
        self._output_mode = OutputMode(mode)
        return self

    @property
    @log()
    def _config(self) -> Config:
        return Config(
            write_options=self._options,
            output_mode=self._output_mode,
        )

    @log()
    def save(self, path: None | str = None) -> "QueryManager":
        sink: Sink
        match self._format:
            case "console":
                sink = ConsoleSink(self._config, self._df)
            case "csv" | "parquet" | "json":
                if not path:
                    raise ValueError(f"Expected a path for {self._format} format")
                sink = FileSink(self._config, self._df, self._format, Path(path))
            case _:
                raise ValueError(f"{self._format} is not supported")
        return sink.save()


class QueryManager:
    def __init__(self, query_process: Process):
        self._query_process = query_process

    @log()
    def stop(self) -> None:
        self._query_process.terminate()
        self._query_process.join()
