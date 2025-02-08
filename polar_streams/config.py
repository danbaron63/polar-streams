from dataclasses import dataclass
from enum import Enum


class OutputMode(Enum):
    COMPLETE = "complete"
    APPEND = "append"
    UPDATE = "update"


@dataclass
class Config:
    write_options: dict[str, str]
    output_mode: OutputMode
