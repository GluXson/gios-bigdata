import json

import polars as pl


try:
    from .pipeline_config import ROOT, RAW_DIR, BRONZE_DIR, YEARS, PERIOD
except ImportError:
    from pipeline_config import ROOT, RAW_DIR, BRONZE_DIR, YEARS, PERIOD

BRONZE_PATH = BRONZE_DIR / f"openmeteo_{PERIOD}.parquet"

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
    try:
        from .download_openmeteo import pm25_station_codes
    except ImportError:
        from download_openmeteo import pm25_station_codes
    codes = sorted(set().union(*(pm25_station_codes(year) for year in YEARS)))
    paths = [RAW_DIR / "openmeteo" / str(year) / f"{code}.json" for year in YEARS for code in codes]
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Brak pogody; uruchom notebook 01: {missing}")
    return paths


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
        f"Zapisano: {BRONZE_PATH.relative_to(ROOT).as_posix()} "
        f"({BRONZE_PATH.stat().st_size / 1_000_000:.1f} MB)"
    )

    return weather


if __name__ == "__main__":
    main()
