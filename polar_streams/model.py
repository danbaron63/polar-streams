from dataclasses import dataclass
from enum import Enum
import polars as pl
from datetime import datetime
from pathlib import Path


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
    source_files: list[Path]
    wal_ids: list[int]


@dataclass
class MicroBatch:
    pl_df: pl.LazyFrame
    metadata: Metadata

    def new(self, pl_df: pl.LazyFrame) -> "MicroBatch":
        return MicroBatch(pl_df=pl_df, metadata=self.metadata)
