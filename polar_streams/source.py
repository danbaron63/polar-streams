from abc import ABC, abstractmethod
from polar_streams.dataframe import DataFrame
import polars as pl
from multiprocessing import Queue
from watchdog.events import FileSystemEvent, FileSystemEventHandler, EVENT_TYPE_CREATED
from watchdog.observers import Observer
from pathlib import Path


class Source(ABC):
    def __init__(self, options: dict[str, str]):
        self._options = options

    @abstractmethod
    def load(self, path: None | str) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def process(self):
        raise NotImplementedError


class FileSource(Source):
    _path = None

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
        yield pl.concat(pl.collect_all([pl.scan_csv(p) for p in source_path.iterdir() if not p.is_dir()]))

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
                pl_df = pl.scan_csv(event.src_path)
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
            case "file":
                return FileSource(self._options).load(path)
            case _:
                raise NotImplementedError(f"{self._format} is not implemented")