"""Ekstrakcja macierzy cech i materializacja warstwy Gold (horyzont 24 h)."""
from datetime import datetime, timedelta, timezone
import hashlib
from importlib.metadata import PackageNotFoundError, version
import json
from pathlib import Path
import random

import duckdb
import polars as pl

try:
    from .pipeline_config import GOLD_DIR, PERIOD, ROOT, SILVER_DIR
except ImportError:
    from pipeline_config import GOLD_DIR, PERIOD, ROOT, SILVER_DIR

DEFAULT_SOURCE = SILVER_DIR / f"pm25_pogoda_{PERIOD}.parquet"
SAMPLE_SIZE = 500_000
BLOCKS_PER_QUARTER = 5
EMBARGO_HOURS = 168
HORIZON_HOURS = 24
DEFAULT_SEED = 42

TARGET = "pm25"
KEY_COLUMNS = ["kod_stacji", "data_czas_utc"]
LEAKAGE_COLUMNS_24H = [
    "pm25_lag1",
    "pm25_lag2",
    "pm25_lag3",
    "pm25_srednia3h",
    "pm25_srednia6h",
    "pm25_srednia24h",
    "pm25_max24h",
]
DROPPED_RAW_COLUMNS = ["wind_direction_10m"]
EXCLUDED_COLUMNS = {TARGET, *KEY_COLUMNS, *LEAKAGE_COLUMNS_24H, *DROPPED_RAW_COLUMNS}

STATION_TYPES = ["tło", "komunikacyjna", "przemysłowa"]
AREA_TYPES = ["miejski", "podmiejski", "pozamiejski"]
STATION_TYPE_ENUM = pl.Enum(STATION_TYPES)
AREA_TYPE_ENUM = pl.Enum(AREA_TYPES)

FEATURE_COLUMNS = [
    # Pogoda surowa (reanaliza Open-Meteo w godzinie t)
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "surface_pressure",
    "wind_speed_10m",
    # Pogoda pochodna i ogrzewanie
    "wiatr_u",
    "wiatr_v",
    "cisnienie_zmiana3h",
    "temperatura_zmiana24h",
    "stopniodni_grzania",
    "opad_suma6h",
    "opad_suma24h",
    # Kalendarz i kodowanie cykliczne (czas lokalny Europe/Warsaw)
    "godzina",
    "dzien_tygodnia",
    "miesiac",
    "weekend",
    "swieto",
    "dzien_wolny",
    "godzina_sin",
    "godzina_cos",
    "miesiac_sin",
    "miesiac_cos",
    # Stacja i siatka
    "typ_stacji",
    "typ_obszaru",
    "wysokosc_m",
    "siatka_szerokosc",
    "siatka_dlugosc",
    # Historia PM2.5 bezpieczna dla horyzontu 24 h
    "pm25_lag24",
    "pm25_lag168",
]
GOLD_COLUMNS = [*KEY_COLUMNS, TARGET, *FEATURE_COLUMNS]


def _check_int(value, name, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} musi być liczbą całkowitą")
    if value < minimum:
        raise ValueError(f"{name} musi być >= {minimum}")
    return value


def _sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _package_version(name):
    try:
        return version(name)
    except PackageNotFoundError:
        return None


def sample_gold(source=DEFAULT_SOURCE, sample_size=SAMPLE_SIZE, seed=DEFAULT_SEED):
    """Pobiera przez DuckDB deterministyczną próbkę reservoir (bez braków celu)."""
    sample_size = _check_int(sample_size, "sample_size", 1)
    seed = _check_int(seed, "seed", 0)
    source = Path(source)
    if not source.is_file():
        raise FileNotFoundError(f"Brak pliku źródłowego silver: {source}")

    selected = ", ".join(GOLD_COLUMNS)
    source_sql = "'" + source.resolve().as_posix().replace("'", "''") + "'"
    # Filtr pm25 IS NOT NULL w podzapytaniu, żeby reservoir losował pełne sample_size wierszy.
    query = f"""
        SELECT *
        FROM (
            SELECT {selected}
            FROM read_parquet({source_sql})
            WHERE {TARGET} IS NOT NULL
        )
        USING SAMPLE reservoir({sample_size} ROWS) REPEATABLE ({seed})
    """

    with duckdb.connect() as con:
        con.execute("SET threads=1")
        frame = con.sql(query).pl()

    if frame.height != sample_size:
        raise ValueError(f"Oczekiwano {sample_size} wierszy w próbce, pobrano {frame.height}")
    if frame[TARGET].null_count() or any(frame[c].null_count() for c in KEY_COLUMNS):
        raise ValueError("Próbka zawiera NULL w kolumnie celu lub klucza")

    return frame.with_columns(
        pl.col("typ_stacji").cast(STATION_TYPE_ENUM, strict=True),
        pl.col("typ_obszaru").cast(AREA_TYPE_ENUM, strict=True),
    ).sort(KEY_COLUMNS)


def split_quarterly_blocks(
    sample,
    blocks=BLOCKS_PER_QUARTER,
    embargo_hours=EMBARGO_HOURS,
    seed=DEFAULT_SEED,
):
    """Dzieli kwartały na bloki czasowe (~18 dni), losuje 1/5 do testu i nakłada embargo po teście."""
    blocks = _check_int(blocks, "blocks", 2)
    embargo_hours = _check_int(embargo_hours, "embargo_hours", 0)
    seed = _check_int(seed, "seed", 0)
    if sample.is_empty() or any(sample[c].null_count() for c in KEY_COLUMNS):
        raise ValueError("Pusta próbka lub NULL w kluczu stacja-czas UTC")

    sample = sample.sort(KEY_COLUMNS)
    quarters = (
        sample
        .select(
            pl.col("data_czas_utc").dt.year().alias("rok"),
            pl.col("data_czas_utc").dt.quarter().alias("kwartal"),
        )
        .unique()
        .sort("rok", "kwartal")
    )

    rng = random.Random(seed)
    ts = pl.col("data_czas_utc")
    is_test = pl.lit(False)
    raw_embargo = pl.lit(False)
    test_blocks = []

    for rok, kwartal in quarters.iter_rows():
        q_start = datetime(rok, (kwartal - 1) * 3 + 1, 1)
        next_q_start = datetime(rok + 1, 1, 1) if kwartal == 4 else datetime(rok, kwartal * 3 + 1, 1)
        q_hours = int((next_q_start - q_start).total_seconds() // 3600)
        tb = rng.randrange(blocks)
        h_start = (tb * q_hours + blocks - 1) // blocks
        h_end = ((tb + 1) * q_hours + blocks - 1) // blocks
        t_start = q_start + timedelta(hours=h_start)
        t_end = q_start + timedelta(hours=h_end)
        embargo_end = t_end + timedelta(hours=embargo_hours)

        is_test = is_test | ((ts >= t_start) & (ts < t_end))
        if embargo_hours > 0:
            raw_embargo = raw_embargo | ((ts >= t_end) & (ts < embargo_end))
        test_blocks.append({
            "year": int(rok),
            "quarter": int(kwartal),
            "test_block": int(tb),
            "test_start_utc": t_start.isoformat(),
            "test_end_utc": t_end.isoformat(),
            "embargo_end_utc": embargo_end.isoformat(),
        })

    flags = sample.select(
        is_test.alias("_test"),
        ((~is_test) & raw_embargo).alias("_embargo"),
    )
    test_df = sample.filter(flags["_test"])
    train_df = sample.filter(~flags["_test"] & ~flags["_embargo"])
    split_info = {
        "embargo_purged": int(flags["_embargo"].sum()),
        "test_blocks": test_blocks,
    }
    return train_df, test_df, split_info


def to_xy(frame):
    """Wydziela bezpieczną macierz cech X (bez kolumn wycieku) i wektor celu y."""
    if frame[TARGET].null_count():
        raise ValueError("Kolumna docelowa pm25 zawiera NULL")
    X = frame.select(FEATURE_COLUMNS)
    leaked = set(X.columns) & EXCLUDED_COLUMNS
    if leaked:
        raise ValueError(f"Wykryto niedozwolone kolumny w X: {sorted(leaked)}")
    return X, frame[TARGET].to_numpy()


def build_gold(
    source=DEFAULT_SOURCE,
    gold_dir=GOLD_DIR,
    sample_size=SAMPLE_SIZE,
    blocks=BLOCKS_PER_QUARTER,
    embargo_hours=EMBARGO_HOURS,
    seed=DEFAULT_SEED,
):
    """Materializuje atomowo warstwę Gold: train.parquet, test.parquet oraz metadata.json."""
    source = Path(source).resolve()
    gold_dir = Path(gold_dir).resolve()

    sample = sample_gold(source=source, sample_size=sample_size, seed=seed)
    train_df, test_df, split_info = split_quarterly_blocks(
        sample, blocks=blocks, embargo_hours=embargo_hours, seed=seed
    )
    to_xy(train_df)
    to_xy(test_df)

    retained = train_df.height + test_df.height
    if retained == 0:
        raise ValueError("Brak wierszy po podziale na train/test")

    source_label = source.relative_to(ROOT).as_posix() if source.is_relative_to(ROOT) else source.as_posix()
    metadata = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "horizon_hours": HORIZON_HOURS,
        "source": {
            "path": source_label,
            "size_bytes": source.stat().st_size,
            "sha256": _sha256_file(source),
        },
        "sampling": {
            "method": "duckdb_reservoir",
            "sample_size": int(sample.height),
            "seed": int(seed),
            "threads": 1,
        },
        "split": {
            "method": "quarterly_blocks_with_post_test_embargo",
            "blocks_per_quarter": int(blocks),
            "embargo_hours": int(embargo_hours),
            "seed": int(seed),
            "test_blocks": split_info["test_blocks"],
        },
        "rows": {
            "sampled": int(sample.height),
            "train": int(train_df.height),
            "test": int(test_df.height),
            "embargo_purged": int(split_info["embargo_purged"]),
            "train_share": round(train_df.height / retained, 6),
            "test_share": round(test_df.height / retained, 6),
        },
        "schema": {
            "key_columns": KEY_COLUMNS,
            "target": TARGET,
            "feature_columns": FEATURE_COLUMNS,
            "excluded_columns": sorted(EXCLUDED_COLUMNS),
            "station_types": STATION_TYPES,
            "area_types": AREA_TYPES,
        },
        "versions": {
            pkg: _package_version(pkg)
            for pkg in ("duckdb", "polars", "pyarrow", "scikit-learn")
        },
    }

    gold_dir.mkdir(parents=True, exist_ok=True)
    train_path = gold_dir / "train.parquet"
    test_path = gold_dir / "test.parquet"
    meta_path = gold_dir / "metadata.json"
    temps = [p.with_name(p.name + ".part") for p in (train_path, test_path, meta_path)]
    try:
        train_df.select(GOLD_COLUMNS).write_parquet(temps[0], compression="snappy")
        test_df.select(GOLD_COLUMNS).write_parquet(temps[1], compression="snappy")
        temps[2].write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        for temp, target in zip(temps, (train_path, test_path, meta_path)):
            temp.replace(target)
    finally:
        for temp in temps:
            temp.unlink(missing_ok=True)

    return train_df, test_df, metadata


def load_gold(gold_dir=GOLD_DIR, return_frames=False):
    """Wczytuje zmaterializowaną warstwę Gold i sprawdza kontrakt schematu oraz brak wycieku."""
    gold_dir = Path(gold_dir)
    train_path = gold_dir / "train.parquet"
    test_path = gold_dir / "test.parquet"
    meta_path = gold_dir / "metadata.json"
    for path in (meta_path, train_path, test_path):
        if not path.is_file():
            raise FileNotFoundError(f"Brak artefaktu warstwy Gold: {path}")

    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    schema = metadata.get("schema", {})
    if (
        schema.get("target") != TARGET
        or schema.get("key_columns") != KEY_COLUMNS
        or schema.get("feature_columns") != FEATURE_COLUMNS
        or schema.get("station_types") != STATION_TYPES
        or schema.get("area_types") != AREA_TYPES
        or not set(schema.get("feature_columns", [])).isdisjoint(EXCLUDED_COLUMNS)
    ):
        raise ValueError("Niezgodny kontrakt schematu w metadata.json")

    train_df = pl.read_parquet(train_path)
    test_df = pl.read_parquet(test_path)

    if train_df.columns != GOLD_COLUMNS or test_df.columns != GOLD_COLUMNS:
        raise ValueError("Niezgodna lista lub kolejność kolumn w plikach Parquet Gold")
    if (
        train_df.schema["typ_stacji"] != STATION_TYPE_ENUM
        or test_df.schema["typ_stacji"] != STATION_TYPE_ENUM
        or train_df.schema["typ_obszaru"] != AREA_TYPE_ENUM
        or test_df.schema["typ_obszaru"] != AREA_TYPE_ENUM
    ):
        raise ValueError("Niezgodne typy pl.Enum w plikach Parquet Gold")

    rows = metadata.get("rows", {})
    if train_df.height != rows.get("train") or test_df.height != rows.get("test"):
        raise ValueError("Liczba wierszy train/test nie zgadza się z metadata.json")
    if (
        train_df[TARGET].null_count()
        or test_df[TARGET].null_count()
        or any(train_df[c].null_count() or test_df[c].null_count() for c in KEY_COLUMNS)
    ):
        raise ValueError("Warstwa Gold zawiera NULL w kolumnie celu lub klucza")
    if set(train_df["data_czas_utc"].to_list()) & set(test_df["data_czas_utc"].to_list()):
        raise ValueError("Wykryto wspólne godziny między zbiorem treningowym i testowym")

    if return_frames:
        return train_df, test_df, metadata

    X_train, y_train = to_xy(train_df)
    X_test, y_test = to_xy(test_df)
    return X_train, X_test, y_train, y_test


def extract_feature_matrix(
    source=DEFAULT_SOURCE,
    sample_size=SAMPLE_SIZE,
    blocks=BLOCKS_PER_QUARTER,
    embargo_hours=EMBARGO_HOURS,
    seed=DEFAULT_SEED,
):
    """Buduje próbkowaną macierz cech i zwraca (X_train, X_test, y_train, y_test)."""
    sample = sample_gold(source=source, sample_size=sample_size, seed=seed)
    train_df, test_df, _ = split_quarterly_blocks(
        sample, blocks=blocks, embargo_hours=embargo_hours, seed=seed
    )
    X_train, y_train = to_xy(train_df)
    X_test, y_test = to_xy(test_df)
    return X_train, X_test, y_train, y_test


def main():
    _, _, metadata = build_gold()
    train_df, test_df, _ = load_gold(return_frames=True)
    X_train, y_train = to_xy(train_df)
    X_test, y_test = to_xy(test_df)

    train_ts = set(train_df["data_czas_utc"].to_list())
    test_ts = set(test_df["data_czas_utc"].to_list())
    lag24_overlap = len({t - timedelta(hours=24) for t in train_ts} & test_ts)
    lag168_overlap = len({t - timedelta(hours=168) for t in train_ts} & test_ts)
    rows = metadata["rows"]

    print(f"Próbka DuckDB reservoir: {rows['sampled']:,} wierszy")
    print(
        f"Train: {X_train.shape} ({100 * rows['train_share']:.2f}%) | "
        f"Test: {X_test.shape} ({100 * rows['test_share']:.2f}%) | "
        f"Embargo ({EMBARGO_HOURS} h): {rows['embargo_purged']:,}"
    )
    print(
        f"Wspólne godziny: {len(train_ts & test_ts)} | "
        f"Wyciek lag24: {lag24_overlap} | Wyciek lag168: {lag168_overlap}"
    )
    print(f"Zapisano warstwę Gold: {GOLD_DIR.relative_to(ROOT).as_posix()}")
    return X_train, X_test, y_train, y_test


if __name__ == "__main__":
    main()
