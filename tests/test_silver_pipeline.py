import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import duckdb
from scripts import silver_features as features
from scripts import partition_benchmark as benchmark


class FeaturesTests(unittest.TestCase):
    def test_new_year_in_local_time(self):
        frame = pl.DataFrame({"data_czas_utc": [datetime(2024, 12, 31, 23)]})
        result = frame.with_columns(*features.calendar_features()).row(0, named=True)
        self.assertTrue(result["swieto"])
        self.assertTrue(result["dzien_wolny"])
        self.assertEqual(result["godzina"], 0)

    def test_grid_rejects_missing_and_duplicate_hours(self):
        for hours in [[0, 2], [0, 0]]:
            base = pl.DataFrame({"kod_stacji": ["A", "A"],
                "data_czas_utc": [datetime(2024, 1, 1) + timedelta(hours=h) for h in hours],
                "typ_stacji": ["tlo", "tlo"], "typ_obszaru": ["miejski", "miejski"]})
            with self.assertRaises(ValueError):
                features.validate_base(base)

    def test_lag_preserves_gap_and_station_boundary(self):
        base = pl.DataFrame({"kod_stacji": ["A"] * 3 + ["B"] * 3,
                            "pm25": [10., None, 30., 100., 200., 300.]})
        result = base.with_columns(*features.pm25_features())
        self.assertEqual(result["pm25_lag1"].to_list(), [None, 10., None, None, 100., 200.])


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        silver = self.root / "data/silver"
        silver.mkdir(parents=True)
        self.frame = pl.DataFrame({
            "kod_stacji": ["A", "A", "B", "B"],
            "data_czas_utc": [datetime(2024, 1, 1), datetime(2024, 1, 1, 1)] * 2,
            "pm25": [10., None, 20., 30.], "godzina": [1, 2, 1, 2],
            "temperature_2m": [1., 2., 3., 4.], "pm25_lag24": [None, 8., None, 9.],
            "extra_feature": [True, None, False, True]})
        self.frame = pl.concat([self.frame, self.frame.filter(pl.col("kod_stacji") == "A").with_columns(pl.lit("C").alias("kod_stacji"))])
        self.frame.write_parquet(silver / "pm25_pogoda_2024.parquet")
        self.ctx = benchmark.create_context(self.root, repeats=2, warmups=0, validation_mode="full", group_count=1, scenario_sizes=(1, 2))
        benchmark.write_variants(self.ctx)

    def tearDown(self):
        self.ctx['con'].close()
        benchmark.release_lock(self.ctx)
        self.tmp.cleanup()

    def test_roundtrip_all_variants(self):
        result = benchmark.verify_data(self.ctx)
        self.assertEqual(result['checked_columns'], 7)
        self.assertTrue(result['full_value_equality_checked'])
        self.assertEqual(result['A_rows'], self.frame.height)
        self.assertEqual(result['B_rows'], self.frame.height)
        self.assertEqual(result['C_rows'], self.frame.height)

    def test_detects_change_to_feature_not_used_in_queries(self):
        self.frame.with_columns(pl.lit(False).alias('extra_feature')).write_parquet(self.ctx['flat'])
        with self.assertRaisesRegex(AssertionError, 'Rozne dane'):
            benchmark.verify_data(self.ctx)

    def test_detects_missing_column(self):
        self.frame.drop('extra_feature').write_parquet(self.ctx['flat'])
        with self.assertRaisesRegex(duckdb.BinderException, 'extra_feature'):
            benchmark.verify_data(self.ctx)


if __name__ == '__main__':
    unittest.main()
