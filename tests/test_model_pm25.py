import tempfile
import unittest
from pathlib import Path

import numpy as np

from scripts import gold_features as gold
from scripts import model_pm25 as modeling

try:
    from tests.test_gold_features import _build_silver_fixture
except ImportError:
    from test_gold_features import _build_silver_fixture


class ModelPm25Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.fixture_path = self.root / "silver_fixture.parquet"
        self.gold_dir = self.root / "gold"
        _build_silver_fixture(self.fixture_path)
        gold.build_gold(
            source=self.fixture_path,
            gold_dir=self.gold_dir,
            sample_size=1500,
            blocks=5,
            embargo_hours=168,
            seed=42,
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_baseline_features_are_safe_raw_subset(self):
        self.assertEqual(len(modeling.BASELINE_FEATURES), 8)
        self.assertTrue(set(modeling.BASELINE_FEATURES).issubset(gold.FEATURE_COLUMNS))
        self.assertTrue(set(modeling.BASELINE_FEATURES).isdisjoint(gold.EXCLUDED_COLUMNS))
        for engineered in ("pm25_lag24", "pm25_lag168", "typ_stacji", "stopniodni_grzania"):
            self.assertNotIn(engineered, modeling.BASELINE_FEATURES)

    def test_train_baseline_rejects_leakage_feature(self):
        X_train, _, y_train, _ = gold.load_gold(self.gold_dir)
        with self.assertRaisesRegex(ValueError, "niedozwolone cechy"):
            modeling.train_baseline(X_train, y_train, features=["temperature_2m", "pm25_lag1"])

    def test_run_iteration1_returns_metrics_and_diagnostics(self):
        res = modeling.run_iteration1(gold_dir=self.gold_dir, seed=42)
        metrics = res["metrics"]
        self.assertEqual(metrics.height, 3)
        self.assertEqual(
            metrics["model"].to_list(),
            [
                "Dummy (średnia z train)",
                "Persystencja 24 h (pm25_lag24)",
                "Iteracja 1 — Baseline (HGBR, 8 cech)",
            ],
        )
        for row in metrics.iter_rows(named=True):
            self.assertGreaterEqual(row["MAE"], 0.0)
            self.assertGreaterEqual(row["RMSE"], row["MAE"])
            self.assertLessEqual(row["R2"], 1.0)

        self.assertEqual(len(res["y_pred"]), len(res["y_test"]))
        self.assertFalse(np.isnan(res["y_pred"]).any())

        diag = res["diagnostics"]
        self.assertEqual(diag["by_pm25_bin"]["n"].sum(), len(res["y_test"]))
        self.assertEqual(diag["by_quarter"]["n"].sum(), len(res["y_test"]))
        self.assertEqual(diag["by_station_type"]["n"].sum(), len(res["y_test"]))
        self.assertGreater(diag["stations"].height, 0)


if __name__ == "__main__":
    unittest.main()
