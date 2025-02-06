from polar_streams.sink import SinkFactory
from abc import ABC, abstractmethod
import polars as pl
from polars.expr.expr import Expr
from typing import Generator

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

    def group_by(self, *cols):
        return GroupedDataFrame(self, list(cols))


class GroupedDataFrame(DataFrame):
    def __init__(self, source, group_cols: list[COL_TYPE]):
        super().__init__(source)
        self._agg_cols = None
        self._group_cols = group_cols
        self._state_pl_df = None

    def agg(self, *cols: list[COL_TYPE]):
        self._agg_cols = cols
        return DataFrame(self)

    def process(self) -> Generator[pl.LazyFrame, None, None]:
        for pl_df in self._source.process():
            if self._state_pl_df is None:
                new_state = pl_df
                self._state_pl_df = new_state
            else:
                new_state = pl.concat([pl_df, self._state_pl_df.lazy()])
            yield new_state.group_by(self._group_cols).agg(self._agg_cols).lazy()


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