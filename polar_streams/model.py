from dataclasses import dataclass
from enum import Enum
import polars as pl
from datetime import datetime


class OutputMode(Enum):
    COMPLETE = "complete"
    APPEND = "append"
    UPDATE = "update"


@dataclass
class Config:
    write_options: dict[str, str]
    output_mode: OutputMode


@dataclass
class Metadata:
    start_time: datetime
    source_files: list[str] = None
    wal_ids: list[int] = None


@dataclass
class MicroBatch:
    pl_df: pl.LazyFrame
    metadata: Metadata = None

    def new(self, pl_df: pl.LazyFrame):
        return MicroBatch(pl_df=pl_df, metadata=self.metadata)
