from abc import ABC, abstractmethod
from typing import Generator

import polars as pl
from polars.expr.expr import Expr

from polar_streams.model import Config, MicroBatch
from polar_streams.sink import SinkFactory
from polar_streams.statestore import StateStore

COL_TYPE = Expr | str


class DataFrame:
    def __init__(self, source):
        self._source = source
        self._operation = None
        self._config = None

    def write_stream(self) -> SinkFactory:
        return SinkFactory(self)

    def process(self) -> Generator[MicroBatch, None, None]:
        for microbatch in self._source.process():
            if self._operation:
                yield self._operation.process(microbatch)
            else:
                yield microbatch

    def set_config(self, config: Config):
        self._config = config
        self._source.set_config(config)

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

    def filter(self, predicate: Expr | bool):
        self._operation = Filter(predicate)
        return DataFrame(self)

    def drop_duplicates(self, *key):
        self._operation = DropDuplicates(list(key))
        return DataFrame(self)


class GroupedDataFrame(DataFrame):
    def __init__(self, source, group_cols: list[COL_TYPE]):
        super().__init__(source)
        self._agg_cols: list[COL_TYPE] = []
        self._group_cols = group_cols
        self._state_store = StateStore("state")

    def agg(self, *cols: list[COL_TYPE]):
        self._agg_cols = list(cols)  # type: ignore
        return DataFrame(self)

    def process(self) -> Generator[MicroBatch, None, None]:
        for microbatch in self._source.process():
            # Fetch state if exists, otherwise initialise with current batch
            if not self._state_store.state_exists("group_by"):
                new_state = microbatch.pl_df.select(*self._group_cols, *self._agg_cols)
            else:
                new_state = pl.concat(
                    [
                        microbatch.pl_df.select(*self._group_cols, *self._agg_cols),
                        self._state_store.get_state("group_by"),
                    ]
                )

            # Update state
            self._state_store.write_state(new_state, "group_by")

            # Yield aggregated result
            yield microbatch.new(
                new_state.group_by(self._group_cols).agg(self._agg_cols).lazy()
            )


class Operator(ABC):
    @abstractmethod
    def process(self, microbatch: MicroBatch) -> MicroBatch:
        raise NotImplementedError


class AddColumns(Operator):
    def __init__(self, cols: list[COL_TYPE]):
        self._cols = cols

    def process(self, microbatch: MicroBatch) -> MicroBatch:
        return microbatch.new(microbatch.pl_df.with_columns(self._cols))


class Select(Operator):
    def __init__(self, cols: list[COL_TYPE]):
        self._cols = cols

    def process(self, microbatch: MicroBatch) -> MicroBatch:
        return microbatch.new(microbatch.pl_df.select(self._cols))


class Filter(Operator):
    def __init__(self, predicate: Expr | bool):
        self._predicate = predicate

    def process(self, microbatch: MicroBatch) -> MicroBatch:
        return microbatch.new(microbatch.pl_df.filter(self._predicate))


class DropDuplicates(Operator):
    def __init__(self, key: list[COL_TYPE]):
        self._key = key
        self._state_store = StateStore()

    def process(self, microbatch: MicroBatch) -> MicroBatch:
        if not self._state_store.state_exists("drop_duplicates"):
            # initialise state
            self._state_store.write_state(
                microbatch.pl_df.select(*self._key).unique(), "drop_duplicates"
            )
            return microbatch.new(microbatch.pl_df.unique(subset=self._key))  # type: ignore

        # deduplicate incoming batch
        pl_df_unique = microbatch.pl_df.unique(subset=self._key)  # type: ignore
        state = self._state_store.get_state("drop_duplicates")

        # filter out records based on state
        pl_df_deduplicated = pl_df_unique.join(
            other=state,
            on=self._key,
            how="anti",
        )

        # update state
        new_state = pl.concat([state, pl_df_unique.select(*self._key)]).unique(
            subset=self._key  # type: ignore
        )

        self._state_store.write_state(new_state, "drop_duplicates")

        # return deduplicated dataframe
        return microbatch.new(pl_df=pl_df_deduplicated)
