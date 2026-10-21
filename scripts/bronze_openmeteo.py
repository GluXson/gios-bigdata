import json
from pathlib import Path

import polars as pl


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw" / "openmeteo" / "2024"
BRONZE_PATH = ROOT / "data" / "bronze" / "openmeteo_2024.parquet"

WEATHER_COLUMNS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
]


def json_to_bronze(path):
    payload = json.loads(path.read_text(encoding="utf-8"))

    hourly = pl.DataFrame(payload["hourly"])

    return hourly.select(
        pl.lit(path.stem).alias("kod_stacji"),
        pl.col("time").str.to_datetime("%Y-%m-%dT%H:%M").alias("data_czas_utc"),
        *[pl.col(name).cast(pl.Float64) for name in WEATHER_COLUMNS],
        pl.lit(payload["latitude"]).alias("siatka_szerokosc"),
        pl.lit(payload["longitude"]).alias("siatka_dlugosc"),
        pl.lit(payload["elevation"]).alias("wysokosc_m"),
        pl.lit(path.name).alias("plik_zrodlowy"),
    )


def weather_files():
    return sorted(
        path
        for path in RAW_DIR.glob("*.json")
        if not path.name.endswith(".metadata.json")
    )


def main():
    paths = weather_files()

    if not paths:
        raise FileNotFoundError(
            f"Brak plików JSON w {RAW_DIR}. Uruchom najpierw notebook 01."
        )

    weather = pl.concat([json_to_bronze(path) for path in paths])

    BRONZE_PATH.parent.mkdir(parents=True, exist_ok=True)
    weather.write_parquet(BRONZE_PATH)

    print(f"Pliki JSON: {len(paths)}")
    print("RAZEM:", weather.shape)
    print(
        f"Zapisano: {BRONZE_PATH} "
        f"({BRONZE_PATH.stat().st_size / 1_000_000:.1f} MB)"
    )

    return weather


if __name__ == "__main__":
    main()
