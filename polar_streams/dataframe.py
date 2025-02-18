import logging
from abc import ABC, abstractmethod
from typing import Generator

import polars as pl
from polars.expr.expr import Expr

from polar_streams.model import Config, MicroBatch, OutputMode
from polar_streams.sink import SinkFactory
from polar_streams.statestore import StateStore
from polar_streams.util import log

logger = logging.getLogger(__name__)
COL_TYPE = Expr | str


class DataFrame:
    def __init__(self, source):
        self._source = source
        self._operation = None

    @log()
    def write_stream(self) -> SinkFactory:
        return SinkFactory(self)

    @log()
    def process(
        self, state_store: StateStore, config: Config
    ) -> Generator[MicroBatch, None, None]:
        for microbatch in self._source.process(state_store, config):
            if self._operation:
                yield self._operation.process(microbatch, state_store)
            else:
                yield microbatch

    @log()
    def with_columns(self, *cols: COL_TYPE):
        self._operation = AddColumns(list(cols))
        return DataFrame(self)

    @log()
    def with_column(self, col: COL_TYPE):
        return self.with_columns(col)

    @log()
    def select(self, *cols: COL_TYPE):
        self._operation = Select(list(cols))
        return DataFrame(self)

    @log()
    def group_by(self, *cols):
        return GroupedDataFrame(self, list(cols))

    @log()
    def filter(self, predicate: Expr | bool):
        self._operation = Filter(predicate)
        return DataFrame(self)

    @log()
    def drop_duplicates(self, *key):
        self._operation = DropDuplicates(list(key))
        return DataFrame(self)


class GroupedDataFrame(DataFrame):
    def __init__(self, source, group_cols: list[COL_TYPE]):
        super().__init__(source)
        self._agg_cols: list[COL_TYPE] = []
        self._group_cols = group_cols

    @log()
    def agg(self, *cols: list[COL_TYPE]):
        self._agg_cols = list(cols)  # type: ignore
        return DataFrame(self)

    @log()
    def process(
        self, state_store: StateStore, config: Config
    ) -> Generator[MicroBatch, None, None]:
        for microbatch in self._source.process(state_store, config):
            # Fetch state if exists, otherwise initialise with current batch
            new_state = microbatch.pl_df
            microbatch_keys = microbatch.pl_df.select(self._group_cols).unique()

            if state_store.state_exists("group_by"):
                new_state = pl.concat(
                    [
                        new_state,
                        state_store.get_state("group_by"),
                    ]
                )

            # Update state
            state_store.write_state(new_state, "group_by")

            result = new_state.group_by(self._group_cols).agg(self._agg_cols).lazy()

            # If update mode, remove unchanged records
            if config.output_mode == OutputMode.UPDATE:
                result = microbatch_keys.join(
                    result, on=self._group_cols, how="left", coalesce=True
                )
            # TODO: implement watermark for late records when using OutputMode.APPEND

            # Yield aggregated result
            yield microbatch.new(result)


class Operator(ABC):
    @abstractmethod
    def process(self, microbatch: MicroBatch, state_store: StateStore) -> MicroBatch:
        raise NotImplementedError


class AddColumns(Operator):
    def __init__(self, cols: list[COL_TYPE]):
        self._cols = cols

    @log()
    def process(self, microbatch: MicroBatch, state_store: StateStore) -> MicroBatch:
        return microbatch.new(microbatch.pl_df.with_columns(self._cols))


class Select(Operator):
    def __init__(self, cols: list[COL_TYPE]):
        self._cols = cols

    @log()
    def process(self, microbatch: MicroBatch, state_store: StateStore) -> MicroBatch:
        return microbatch.new(microbatch.pl_df.select(self._cols))


class Filter(Operator):
    def __init__(self, predicate: Expr | bool):
        self._predicate = predicate

    @log()
    def process(self, microbatch: MicroBatch, state_store: StateStore) -> MicroBatch:
        return microbatch.new(microbatch.pl_df.filter(self._predicate))


class DropDuplicates(Operator):
    def __init__(self, key: list[COL_TYPE]):
        self._key = key

    @log()
    def process(self, microbatch: MicroBatch, state_store: StateStore) -> MicroBatch:
        if not state_store.state_exists("drop_duplicates"):
            # initialise state
            state_store.write_state(
                microbatch.pl_df.select(*self._key).unique(), "drop_duplicates"
            )
            return microbatch.new(microbatch.pl_df.unique(subset=self._key))  # type: ignore

        # deduplicate incoming batch
        pl_df_unique = microbatch.pl_df.unique(subset=self._key)  # type: ignore
        state = state_store.get_state("drop_duplicates")

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

        state_store.write_state(new_state, "drop_duplicates")

        # return deduplicated dataframe
        return microbatch.new(pl_df=pl_df_deduplicated)
