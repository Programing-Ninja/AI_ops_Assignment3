"""Streaming Ray Data datasource for TLC files with very large single row groups."""
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource, ReadTask

from common import BASE_COLUMNS, arrow_schema


class StreamingTaxiParquet(Datasource):
    """Yield bounded Arrow batches; never read a whole month into an Arrow table."""

    def __init__(self, files, batch_rows):
        self.files = [str(path) for path in files]
        self.batch_rows = batch_rows

    def estimate_inmemory_data_size(self):
        return sum(pq.ParquetFile(filename).metadata.num_rows * 128 for filename in self.files)

    def get_read_tasks(self, parallelism):
        tasks = []
        for filename in self.files:
            parquet = pq.ParquetFile(filename)
            rows = parquet.metadata.num_rows
            batch_rows = self.batch_rows
            def read_file(path=filename, limit=batch_rows):
                from common import arrow_schema
                # One file's physical integer/timestamp types can differ from another's.
                for batch in pq.ParquetFile(path).iter_batches(
                        batch_size=limit, columns=BASE_COLUMNS, use_threads=False):
                    yield pa.Table.from_batches([batch]).cast(arrow_schema("raw"), safe=False)
            metadata = BlockMetadata(num_rows=rows, size_bytes=rows * 128,
                                     exec_stats=None, input_files=[filename])
            tasks.append(ReadTask(read_file, metadata, schema=arrow_schema("raw")))
        return tasks
