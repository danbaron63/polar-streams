import logging
from abc import ABC, abstractmethod
from datetime import datetime
from multiprocessing import Queue
from pathlib import Path
from typing import Generator

import polars as pl
from watchdog.events import EVENT_TYPE_CREATED, FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from polar_streams.dataframe import DataFrame
from polar_streams.model import Config, Metadata, MicroBatch, OutputMode
from polar_streams.statestore import StateStore
from polar_streams.util import log

logger = logging.getLogger(__name__)


class Source(ABC):
    def __init__(self, options: dict[str, str]):
        self._options = options

    @abstractmethod
    def load(self, path: None | str) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def process(self, state_store: StateStore) -> Generator[MicroBatch, None, None]:
        raise NotImplementedError


class FileSource(Source):
    def __init__(self, options: dict[str, str], fmt: str):
        super().__init__(options)
        self._path: None | Path = None
        self._options = options
        self._format = fmt
        match fmt:
            case "csv":
                self._read_func = pl.scan_csv  # type: ignore
            case "parquet":
                self._read_func = pl.scan_parquet  # type: ignore
            case "ndjson":
                self._read_func = pl.scan_ndjson  # type: ignore
            case "json":
                self._read_func = pl.read_json  # type: ignore

    def _read_path(self, path: str) -> pl.LazyFrame:
        return self._read_func(path).lazy()

    @log()
    def load(self, path: None | str) -> DataFrame:
        if not path:
            raise ValueError("Expected a path when calling load()")

        self._path = Path(path)
        df = DataFrame(self)
        return df

    class FileEventHandler(FileSystemEventHandler):
        def __init__(self, q: Queue):
            self._q = q

        def on_any_event(self, event: FileSystemEvent) -> None:
            if event.event_type == EVENT_TYPE_CREATED:
                self._q.put(event)

    @log()
    def process(
        self, state_store: StateStore, config: Config
    ) -> Generator[MicroBatch, None, None]:
        if not self._path:
            raise ValueError("path cannot be of type None")

        # batch process all files and then listen for new ones
        run_initial_batch = self._options.get("run_initial_batch", "true") == "true"
        source_files = [p for p in self._path.iterdir() if not p.is_dir()]
        wal_ids = (state_store.wal_append(p.as_posix()) for p in source_files)
        source_batches = (self._read_path(p.as_posix()) for p in source_files)
        if run_initial_batch:
            yield MicroBatch(
                pl_df=pl.concat(pl.collect_all(list(source_batches))).lazy(),
                metadata=Metadata(
                    source_files=source_files,
                    wal_ids=list(wal_ids),
                    start_time=datetime.now(),
                ),
            )
        else:
            for pl_df, source_file, wal_id in zip(
                source_batches, source_files, wal_ids
            ):
                yield MicroBatch(
                    pl_df=pl_df,
                    metadata=Metadata(
                        source_files=[source_file],
                        wal_ids=[wal_id],
                        start_time=datetime.now(),
                    ),
                )

        # For complete output mode don't create source thread
        if config.output_mode == OutputMode.COMPLETE:
            return

        # search for new files and pass them along using watchdog.
        q: Queue = Queue()
        event_handler = self.FileEventHandler(q)
        observer = Observer()
        observer.schedule(event_handler, self._path.as_posix(), recursive=True)
        observer.start()

        try:
            while True:
                event = q.get()
                wal_id = state_store.wal_append(event.src_path)
                pl_df = self._read_path(event.src_path)
                # TODO: schema check
                yield MicroBatch(
                    pl_df=pl_df,
                    metadata=Metadata(
                        source_files=[event.src_path],
                        wal_ids=[wal_id],
                        start_time=datetime.now(),
                    ),
                )
        finally:
            observer.stop()
            observer.join()


class SourceFactory:
    def __init__(self) -> None:
        self._options: dict[str, str] = dict()
        self._format: str = ""

    @log()
    def option(self, key: str, value: str) -> "SourceFactory":
        self._options[key] = value
        return self

    @log()
    def format(self, fmt: str) -> "SourceFactory":
        self._format = fmt
        return self

    @log()
    def load(self, path: None | str = None) -> DataFrame:
        match self._format:
            case "csv" | "parquet" | "json" | "ndjson":
                return FileSource(self._options, self._format).load(path)
            case _:
                raise ValueError(f"{self._format} is not supported")
