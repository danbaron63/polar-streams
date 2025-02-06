from polar_streams.sink import SinkFactory
from abc import ABC, abstractmethod
import polars as pl
from polars.expr.expr import Expr

COL_TYPE = Expr | str


class DataFrame:
    def __init__(self, source):
        self._source = source
        self._operation = None

    def write_stream(self) -> SinkFactory:
        return SinkFactory(self)

    def process(self):
        for pl_df in self._source.process():
            if self._operation:
                yield self._operation.process(pl_df)
            else:
                yield pl_df

    def with_columns(self, *cols: COL_TYPE):
        self._operation = AddColumns(list(cols))
        return DataFrame(self)

    def with_column(self, col: COL_TYPE):
        return self.with_columns(col)

    def select(self, *cols: COL_TYPE):
        self._operation = Select(list(cols))
        return DataFrame(self)


class Operator(ABC):
    @abstractmethod
    def process(self, pl_df: pl.DataFrame) -> pl.DataFrame:
        raise NotImplementedError


class AddColumns(Operator):
    def __init__(self, cols: list[COL_TYPE]):
        self._cols = cols

    def process(self, pl_df: pl.DataFrame) -> pl.DataFrame:
        return pl_df.with_columns(self._cols)


class Select(Operator):
    def __init__(self, cols: list[COL_TYPE]):
        self._cols = cols

    def process(self, pl_df: pl.DataFrame) -> pl.DataFrame:
        return pl_df.select(self._cols)