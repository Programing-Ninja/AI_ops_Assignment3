import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from common import (BASE_COLUMNS, CONTRACT, add_features, arrow_schema, average_speed,
                    clean_batch, load_zones, prepare_run, parser, write_json)
from make_fixture import make_fixture
from validate_outputs import compare


class CleaningTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="taxi-unit-")
        self.root = Path(self.directory.name)
        make_fixture(self.root / "input")

    def tearDown(self):
        self.directory.cleanup()

    def cleaned(self):
        rows = []
        for path in sorted((self.root / "input/trips").glob("*.parquet")):
            for batch in pq.ParquetFile(path).iter_batches(batch_size=2, columns=BASE_COLUMNS):
                table = pa.Table.from_batches([batch]).cast(arrow_schema("raw"), safe=False)
                rows.extend(clean_batch(table, 8).to_pylist())
        return rows

    def test_null_invalid_and_cross_file_keys(self):
        rows = self.cleaned()
        self.assertEqual(len(rows), 7)
        unique = {row["_key"]: row for row in rows}
        self.assertEqual(len(unique), 4)
        for row in rows:
            hash_value = pa.Table.from_pylist([row], schema=arrow_schema("staging"))
            frame = hash_value.select(BASE_COLUMNS).to_pandas()
            expected_hash = pd.util.hash_pandas_object(
                frame, index=False, hash_key="0123456789abcdef").iloc[0]
            self.assertEqual(row["_bucket"], int(expected_hash % 8))
            self.assertEqual(row["_key"], expected_hash.view("int64"))
        # -0.0 and +0.0 were made identical before hashing.
        self.assertEqual(sum(row["fare_amount"] == 0.0 for row in rows), 2)

    def test_empty_batch_retains_schema(self):
        table = pa.Table.from_batches([], schema=arrow_schema("raw"))
        output = clean_batch(table, 8)
        self.assertEqual(output.num_rows, 0)
        self.assertTrue(output.schema.remove_metadata().equals(arrow_schema("staging")))

    def test_streaming_source_bounds_batches_and_preserves_all_rows(self):
        from ray_clean import StreamingTaxiParquet
        source = StreamingTaxiParquet(sorted((self.root / "input/trips").glob("*.parquet")), 2)
        self.assertFalse(source.should_create_reader)
        tasks = source.get_read_tasks(2)
        self.assertEqual(sum(task.metadata.num_rows for task in tasks), 14)
        batches = [batch for task in tasks for batch in task()]
        self.assertEqual(sum(batch.num_rows for batch in batches), 14)
        self.assertTrue(all(batch.num_rows <= 2 for batch in batches))
        self.assertTrue(all(batch.schema.equals(arrow_schema("raw")) for batch in batches))

    def test_streaming_source_reads_staged_bucket_without_empty_splits(self):
        from ray_clean import StreamingTaxiParquet
        rows = self.cleaned()
        bucket = self.root / "staged" / f"_bucket={rows[0]['_bucket']}"
        bucket.mkdir(parents=True)
        path = bucket / "part-000.parquet"
        pq.write_table(pa.Table.from_pylist(rows[:1], schema=arrow_schema("staging")), path)
        columns = BASE_COLUMNS + ["_key"]
        source = StreamingTaxiParquet([path], 2, columns=columns, schema_kind="staging")
        batches = list(source.get_read_tasks(4)[0]())
        expected = pa.schema([arrow_schema("staging").field(name) for name in columns])
        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0].num_rows, 1)
        self.assertTrue(batches[0].schema.equals(expected))

    def test_bucketed_source_deduplicates_exactly_across_fragments(self):
        from ray_clean import StreamingBucketedParquet
        rows = self.cleaned()
        duplicate = rows[0]
        different = dict(duplicate, fare_amount=duplicate["fare_amount"] + 1.0,
                         _key=duplicate["_key"] ^ 1)
        bucket = duplicate["_bucket"]
        directory = self.root / "bucketed" / f"_bucket={bucket}"
        directory.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist([duplicate], schema=arrow_schema("staging")).select(
            BASE_COLUMNS + ["_key"]), directory / "part-0.parquet")
        pq.write_table(pa.Table.from_pylist([duplicate, different], schema=arrow_schema("staging")).select(
            BASE_COLUMNS + ["_key"]), directory / "part-1.parquet")
        source = StreamingBucketedParquet({bucket: sorted(directory.glob("*.parquet"))}, 1)
        output = [batch for batch in source.get_read_tasks(1)[0]()]
        self.assertEqual(sum(batch.num_rows for batch in output), 2)
        self.assertTrue(all(batch.schema.equals(arrow_schema("staging")) for batch in output))

    def test_fractional_duration_and_python_speed(self):
        self.assertEqual(average_speed(3, 600), 18)
        rows = list({row["_key"]: row for row in self.cleaned()}.values())
        row = next(row for row in rows if row["trip_distance"] == 1)
        row.update(pickup_borough="Queens", pickup_zone="Zone A", dropoff_borough="Brooklyn", dropoff_zone="Zone B")
        output = add_features(pa.Table.from_pylist([row])).to_pylist()[0]
        self.assertEqual(output["duration_seconds"], 2.000001)
        self.assertEqual(output["average_speed_mph"], 1799.9991)

    def test_feature_duration_normalizes_second_precision_timestamps(self):
        row = next(row for row in self.cleaned() if row["trip_distance"] == 3.0)
        row.update(pickup_borough="Queens", pickup_zone="Zone A",
                   dropoff_borough="Brooklyn", dropoff_zone="Zone B")
        pickup = row["tpep_pickup_datetime"].replace(hour=0, minute=0, second=0, microsecond=0)
        source = pa.Table.from_pylist([row])
        for name, value in [
            ("tpep_pickup_datetime", pickup),
            ("tpep_dropoff_datetime", pickup + timedelta(minutes=10)),
        ]:
            index = source.schema.get_field_index(name)
            source = source.set_column(index, name, pa.array([value], type=pa.timestamp("s")))
        output = add_features(source).to_pylist()[0]
        self.assertEqual(output["duration_seconds"], 600.0)

    def test_duplicate_zone_lookup_rejected(self):
        lookup = self.root / "input/zones.csv"
        lookup.write_text(lookup.read_text() + "1,Queens,Another zone,Boro Zone\n")
        with self.assertRaisesRegex(ValueError, "Duplicate lookup"):
            load_zones(lookup)

    def test_output_overwrite_rejected(self):
        cli = parser("test")
        output = self.root / "already-exists"
        output.mkdir()
        args = cli.parse_args(["--input", str(self.root / "input/trips"), "--lookup", str(self.root / "input/zones.csv"),
                               "--output", str(output), "--staging", str(self.root / "staging")])
        with self.assertRaises(FileExistsError):
            prepare_run(args, "test")
        self.assertFalse((self.root / "staging").exists())

    def test_uncompressed_budget_selects_same_whole_file_prefix(self):
        files = sorted((self.root / "input/trips").glob("*.parquet"))
        first = pq.ParquetFile(files[0])
        first_size = sum(first.metadata.row_group(i).total_byte_size
                         for i in range(first.metadata.num_row_groups))
        args = parser("test").parse_args([
            "--input", str(self.root / "input/trips"),
            "--lookup", str(self.root / "input/zones.csv"),
            "--max-uncompressed-gb", str(first_size / 1_000_000_000),
            "--output", str(self.root / "budget-output"),
            "--staging", str(self.root / "budget-staging"),
        ])
        selected, _, metadata = prepare_run(args, "test")
        self.assertEqual(selected, [files[0]])
        self.assertEqual(metadata["input_uncompressed_bytes"], first_size)
        self.assertEqual(metadata["input_manifest"][0]["uncompressed_bytes"], first_size)

    def write_golden(self, destination, omit=False, duplicate=False):
        rows = list({row["_key"]: row for row in self.cleaned()}.values())
        rows = [row for row in rows if row["PULocationID"] == 1]
        if omit:
            rows = rows[:-1]
        if duplicate:
            rows.append(rows[0])
        for row in rows:
            row.update(pickup_borough="Queens", pickup_zone="Zone A", dropoff_borough="Brooklyn", dropoff_zone="Zone B")
            target = destination / f"bucket={row['_bucket']}"
            target.mkdir(parents=True, exist_ok=True)
            output = add_features(pa.Table.from_pylist([row]))
            pq.write_table(output, target / f"part-{len(list(target.glob('*.parquet')))}.parquet")
        metadata = {"contract": CONTRACT, "status": "complete", "configuration": {"buckets": 8},
                    "input_manifest": [], "lookup_sha256": "fixture-only", "output_rows": len(rows)}
        write_json(destination / "_run.json", metadata)

    def test_exact_parity_and_missing_row_detection(self):
        left, right, missing = [self.root / name for name in ["left", "right", "missing"]]
        self.write_golden(left)
        self.write_golden(right)
        self.write_golden(missing, omit=True)
        self.assertEqual(compare(left, right, batch_rows=1)["left_rows"], 3)
        with self.assertRaisesRegex(ValueError, "mismatch"):
            compare(left, missing)

    def test_equal_duplicate_outputs_still_fail(self):
        left, right = self.root / "left", self.root / "right"
        self.write_golden(left, duplicate=True)
        self.write_golden(right, duplicate=True)
        with self.assertRaisesRegex(ValueError, "Duplicate output"):
            compare(left, right)


if __name__ == "__main__":
    unittest.main()
