import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
from sklearn.ensemble import HistGradientBoostingRegressor

from scripts import gold_features as gold


def _build_silver_fixture(path):
    """Tworzy syntetyczny plik Silver (40 kolumn, codziennie 2023–2025, 3 stacje)."""
    stations = [
        ("ST_A", "tło", "miejski", 51.1, 17.0, 120.0),
        ("ST_B", "komunikacyjna", "podmiejski", 52.2, 21.0, 100.0),
        ("ST_C", "przemysłowa", "pozamiejski", 50.3, 19.0, 250.0),
    ]
    rows = []
    idx = 0
    day = datetime(2023, 1, 1, 12, 0)
    end = datetime(2025, 12, 31, 12, 0)
    while day <= end:
        for code, st_type, area_type, lat, lon, alt in stations:
            is_null_target = (idx % 5) in (1, 3)  # 40% braków pm25
            pm25_val = None if is_null_target else float(10 + (idx % 30))
            lag24_val = None if idx % 7 == 0 else float(9 + (idx % 25))
            lag168_val = None if idx % 11 == 0 else float(8 + (idx % 20))
            rows.append({
                "kod_stacji": code,
                "data_czas_utc": day,
                "temperature_2m": float(day.month - 2 + (idx % 5)),
                "relative_humidity_2m": 65.0,
                "precipitation": 0.2 if idx % 4 == 0 else 0.0,
                "surface_pressure": 1013.0,
                "wind_speed_10m": 3.5,
                "wind_direction_10m": 180.0,
                "siatka_szerokosc": lat,
                "siatka_dlugosc": lon,
                "wysokosc_m": alt,
                "pm25": pm25_val,
                "typ_stacji": st_type,
                "typ_obszaru": area_type,
                "godzina": 13,
                "dzien_tygodnia": (day.weekday() + 1),
                "miesiac": day.month,
                "weekend": day.weekday() >= 5,
                "swieto": False,
                "dzien_wolny": day.weekday() >= 5,
                "godzina_sin": 0.0,
                "godzina_cos": -1.0,
                "miesiac_sin": 0.5,
                "miesiac_cos": 0.5,
                "pm25_lag1": 999.0,
                "pm25_lag2": 998.0,
                "pm25_lag3": 997.0,
                "pm25_lag24": lag24_val,
                "pm25_lag168": lag168_val,
                "pm25_srednia3h": 996.0,
                "pm25_srednia6h": 995.0,
                "pm25_srednia24h": 994.0,
                "pm25_max24h": 993.0,
                "wiatr_u": 0.0,
                "wiatr_v": 3.5,
                "cisnienie_zmiana3h": 0.5,
                "temperatura_zmiana24h": -1.0,
                "stopniodni_grzania": max(0.0, 15.0 - float(day.month - 2)),
                "opad_suma6h": 0.4,
                "opad_suma24h": 1.2,
            })
            idx += 1
        day += timedelta(days=1)

    frame = pl.DataFrame(rows).with_columns(
        pl.col("godzina").cast(pl.Int8),
        pl.col("dzien_tygodnia").cast(pl.Int8),
        pl.col("miesiac").cast(pl.Int8),
    )
    frame.write_parquet(path)
    return frame


class GoldFeaturesTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.fixture_path = self.root / "silver_fixture.parquet"
        self.full_frame = _build_silver_fixture(self.fixture_path)

    def tearDown(self):
        self.tmp.cleanup()

    def test_reservoir_filters_null_target_and_is_deterministic(self):
        # 1972 niepustych z 3288 wierszy; 1500 > 1315 (60% niepustych z 3288) dowodzi filtra przed losowaniem.
        s1 = gold.sample_gold(self.fixture_path, sample_size=1500, seed=42)
        s2 = gold.sample_gold(self.fixture_path, sample_size=1500, seed=42)
        self.assertEqual(s1.height, 1500)
        self.assertEqual(s1["pm25"].null_count(), 0)
        self.assertGreater(s1["pm25_lag24"].null_count(), 0)
        self.assertTrue(s1.equals(s2))

    def test_quarterly_blocks_and_168h_embargo_prevent_lag_leakage(self):
        non_null = self.full_frame.filter(pl.col("pm25").is_not_null())
        train_df, test_df, split_info = gold.split_quarterly_blocks(
            non_null, blocks=5, embargo_hours=168, seed=42
        )

        self.assertGreater(split_info["embargo_purged"], 0)
        train_q = set(
            train_df.select(
                pl.col("data_czas_utc").dt.year().alias("rok"),
                pl.col("data_czas_utc").dt.quarter().alias("kwartal"),
            ).iter_rows()
        )
        test_q = set(
            test_df.select(
                pl.col("data_czas_utc").dt.year().alias("rok"),
                pl.col("data_czas_utc").dt.quarter().alias("kwartal"),
            ).iter_rows()
        )
        self.assertEqual(len(train_q), 12)
        self.assertEqual(len(test_q), 12)

        train_ts = set(train_df["data_czas_utc"].to_list())
        test_ts = set(test_df["data_czas_utc"].to_list())
        self.assertEqual(train_ts & test_ts, set())
        self.assertEqual({t - timedelta(hours=24) for t in train_ts} & test_ts, set())
        self.assertEqual({t - timedelta(hours=168) for t in train_ts} & test_ts, set())

    def test_build_and_load_gold_validates_contract_and_enums(self):
        gold_dir = self.root / "gold"
        _, _, meta = gold.build_gold(
            source=self.fixture_path,
            gold_dir=gold_dir,
            sample_size=1500,
            blocks=5,
            embargo_hours=168,
            seed=42,
        )

        self.assertTrue((gold_dir / "train.parquet").is_file())
        self.assertTrue((gold_dir / "test.parquet").is_file())
        self.assertTrue((gold_dir / "metadata.json").is_file())
        self.assertEqual(list(gold_dir.glob("*.part")), [])
        self.assertEqual(meta["rows"]["sampled"], 1500)
        self.assertGreater(meta["rows"]["embargo_purged"], 0)

        X_train, X_test, y_train, y_test = gold.load_gold(gold_dir)
        self.assertEqual(X_train.columns, gold.FEATURE_COLUMNS)
        self.assertEqual(X_test.columns, gold.FEATURE_COLUMNS)
        self.assertEqual(len(gold.FEATURE_COLUMNS), 29)
        self.assertTrue(set(X_train.columns).isdisjoint(gold.EXCLUDED_COLUMNS))
        self.assertEqual(X_train.schema["typ_stacji"], gold.STATION_TYPE_ENUM)
        self.assertEqual(X_train.schema["typ_obszaru"], gold.AREA_TYPE_ENUM)
        self.assertEqual(len(y_train), X_train.height)
        self.assertEqual(len(y_test), X_test.height)

        model = HistGradientBoostingRegressor(
            categorical_features="from_dtype", max_iter=5, random_state=42
        )
        model.fit(X_train, y_train)
        self.assertEqual(len(model.predict(X_test)), X_test.height)

        # Kontrakt musi odrzucić uszkodzony/niezgodny metadata.json.
        meta_path = gold_dir / "metadata.json"
        broken = json.loads(meta_path.read_text(encoding="utf-8"))
        broken["rows"]["train"] += 1
        meta_path.write_text(json.dumps(broken), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Liczba wierszy"):
            gold.load_gold(gold_dir)

        broken["rows"]["train"] -= 1
        broken["schema"]["feature_columns"] = [*gold.FEATURE_COLUMNS, "pm25_lag1"]
        meta_path.write_text(json.dumps(broken), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "kontrakt schematu"):
            gold.load_gold(gold_dir)


if __name__ == "__main__":
    unittest.main()
