from polar_streams.sink import SinkFactory


class DataFrame:
    def __init__(self, source):
        self._source = source

    def write_stream(self) -> SinkFactory:
        return SinkFactory(self)

    def process(self):
        for pl_df in self._source.process():
            yield pl_df
