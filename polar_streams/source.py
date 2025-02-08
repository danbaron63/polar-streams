from abc import ABC, abstractmethod
from polar_streams.config import Config, OutputMode
from polar_streams.dataframe import DataFrame
import polars as pl
from multiprocessing import Queue
from watchdog.events import FileSystemEvent, FileSystemEventHandler, EVENT_TYPE_CREATED
from watchdog.observers import Observer
from pathlib import Path


class Source(ABC):
    def __init__(self, options: dict[str, str]):
        self._options = options
        self._config: Config | None = None

    def set_config(self, config: Config):
        self._config = config

    @abstractmethod
    def load(self, path: None | str) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def process(self):
        raise NotImplementedError


class FileSource(Source):
    def __init__(self, options: dict[str, str], fmt: str):
        super().__init__(options)
        self._path = None
        self._options = options
        self._format = fmt
        match fmt:
            case "csv":
                self._read_func = pl.scan_csv
            case "parquet":
                self._read_func = pl.scan_parquet
            case "ndjson":
                self._read_func = pl.scan_ndjson
            case "json":
                self._read_func = pl.read_json

    def _read_path(self, path: str) -> pl.LazyFrame:
        return self._read_func(path).lazy()

    def load(self, path: str) -> DataFrame:
        if not path:
            raise ValueError("Expected a path when calling load()")

        self._path = path
        df = DataFrame(self)
        return df

    class MyEventHandler(FileSystemEventHandler):
        def __init__(self, q: Queue):
            self._q = q

        def on_any_event(self, event: FileSystemEvent) -> None:
            if event.event_type == EVENT_TYPE_CREATED:
                self._q.put(event)

    def process(self):
        # batch process all files and then listen for new ones
        source_path = Path(self._path)
        run_initial_batch = self._options.get("run_initial_batch", "true") == "true"
        source_batches = (self._read_path(p) for p in source_path.iterdir() if not p.is_dir())
        if run_initial_batch:
            yield pl.concat(pl.collect_all(list(source_batches)))
        else:
            for pl_df in source_batches:
                yield pl_df

        # For complete output mode don't create source thread
        if self._config.output_mode == OutputMode.COMPLETE:
            return

        # search for new files and pass them along
        # using watchdog.
        q = Queue()
        event_handler = self.MyEventHandler(q)
        observer = Observer()
        observer.schedule(event_handler, self._path, recursive=True)
        observer.start()

        try:
            while True:
                event = q.get()
                pl_df = self._read_path(event.src_path)
                # TODO: schema check
                yield pl_df
        finally:
            observer.stop()
            observer.join()


class SourceFactory:
    def __init__(self):
        self._options = dict()
        self._format = None

    def option(self, key: str, value: str) -> "SourceFactory":
        self._options[key] = value
        return self

    def format(self, fmt: str) -> "SourceFactory":
        self._format = fmt
        return self

    def load(self, path: None | str = None) -> DataFrame:
        match self._format:
            case "csv" | "parquet" | "json" | "ndjson":
                return FileSource(self._options, self._format).load(path)
            case _:
                raise ValueError(f"{self._format} is not supported")