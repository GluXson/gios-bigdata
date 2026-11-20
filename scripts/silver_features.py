import math
from datetime import date
from pathlib import Path

import polars as pl


ROOT = Path(__file__).resolve().parents[1]
BRONZE_DIR = ROOT / "data" / "bronze"
SILVER_DIR = ROOT / "data" / "silver"

# Dni ustawowo wolne w 2024 r. (Wigilia jest wolna dopiero od 2025 r.).
HOLIDAYS = [
    date(2024, 1, 1), date(2024, 1, 6), date(2024, 3, 31), date(2024, 4, 1),
    date(2024, 5, 1), date(2024, 5, 3), date(2024, 5, 19), date(2024, 5, 30),
    date(2024, 8, 15), date(2024, 11, 1), date(2024, 11, 11),
    date(2024, 12, 25), date(2024, 12, 26),
    date(2025, 1, 1),  # Ostatnia godzina 2024 UTC to już Nowy Rok w Polsce.
]

PM25_LAGS = [1, 2, 3, 24, 168]
PM25_WINDOWS = [3, 6, 24]
RAIN_WINDOWS = [6, 24]
# Temperatura bazowa stopniodni grzania: poniżej niej budynki są ogrzewane.
HEATING_BASE = 15.0

STATION_COLUMNS = ["kod_stacji", "typ_stacji", "typ_obszaru"]


def build_base(measurements, weather, stations):
    """Pogoda jako pełna siatka godzin + PM2.5 + typ stacji."""
    pm25 = (
        measurements
        .filter((pl.col("wskaznik") == "PM2.5") & (pl.col("czas_usredniania") == "1g"))
        .select("kod_stacji", "data_czas_utc", pl.col("wartosc").alias("pm25"))
    )
    stations = (
        stations
        .with_columns(pl.col("kod_stacji").str.strip_chars())
        .unique("kod_stacji", keep="first")
        .select(STATION_COLUMNS)
    )

    # Pogoda jest szkieletem: brak pomiaru PM2.5 to NULL, a nie brak wiersza,
    # więc shift(k) zawsze oznacza k godzin wstecz.
    return (
        weather
        .drop("plik_zrodlowy")
        .join(pm25, on=["kod_stacji", "data_czas_utc"], how="left", validate="1:1")
        .join(stations, on="kod_stacji", how="left", validate="m:1")
        .sort("kod_stacji", "data_czas_utc")
    )


def calendar_features():
    # Aktywność ludzi (ruch, ogrzewanie) idzie za czasem lokalnym, nie UTC.
    local = pl.col("data_czas_utc").dt.replace_time_zone("UTC").dt.convert_time_zone("Europe/Warsaw")
    hour = local.dt.hour()
    month = local.dt.month()
    weekday = local.dt.weekday()
    holiday = local.dt.date().is_in(HOLIDAYS)

    return [
        hour.alias("godzina"),
        weekday.alias("dzien_tygodnia"),
        month.alias("miesiac"),
        (weekday >= 6).alias("weekend"),
        holiday.alias("swieto"),
        ((weekday >= 6) | holiday).alias("dzien_wolny"),
        # Kodowanie cykliczne: 23:00 jest blisko 0:00, grudzień blisko stycznia.
        (2 * math.pi * hour / 24).sin().alias("godzina_sin"),
        (2 * math.pi * hour / 24).cos().alias("godzina_cos"),
        (2 * math.pi * (month - 1) / 12).sin().alias("miesiac_sin"),
        (2 * math.pi * (month - 1) / 12).cos().alias("miesiac_cos"),
    ]


def pm25_features():
    # Tylko przeszłość: okna liczone od t-1, żeby bieżąca wartość nie wyciekła do cech.
    past = pl.col("pm25").shift(1)
    lags = [pl.col("pm25").shift(k).over("kod_stacji").alias(f"pm25_lag{k}") for k in PM25_LAGS]
    means = [
        past.rolling_mean(k, min_samples=1).over("kod_stacji").alias(f"pm25_srednia{k}h")
        for k in PM25_WINDOWS
    ]
    return [
        *lags,
        *means,
        past.rolling_max(24, min_samples=1).over("kod_stacji").alias("pm25_max24h"),
    ]


def weather_features():
    # Kierunek wiatru = skąd wieje; przy ciszy kierunek jest NULL, a składowe 0.
    direction = pl.col("wind_direction_10m").radians()
    speed = pl.col("wind_speed_10m")
    rain = [
        pl.col("precipitation").rolling_sum(k, min_samples=1).over("kod_stacji").alias(f"opad_suma{k}h")
        for k in RAIN_WINDOWS
    ]

    return [
        (-speed * direction.sin()).fill_null(0).alias("wiatr_u"),
        (-speed * direction.cos()).fill_null(0).alias("wiatr_v"),
        pl.col("surface_pressure").diff(3).over("kod_stacji").alias("cisnienie_zmiana3h"),
        pl.col("temperature_2m").diff(24).over("kod_stacji").alias("temperatura_zmiana24h"),
        (HEATING_BASE - pl.col("temperature_2m")).clip(lower_bound=0).alias("stopniodni_grzania"),
        *rain,
    ]


def validate_base(base):
    """Pełna siatka UTC przed shift/rolling; NULL celu pozostają w tabeli."""
    keys = ["kod_stacji", "data_czas_utc"]
    if base.is_empty() or any(base[c].null_count() for c in keys):
        raise ValueError("Pusta siatka lub NULL w kluczu stacja-czas UTC")
    if base.select(pl.struct(keys).is_duplicated().any()).item():
        raise ValueError("Duplikat stacji i czasu UTC")
    if base.select((pl.col("data_czas_utc").diff().over("kod_stacji") != pl.duration(hours=1)).any()).item():
        raise ValueError("Nieciągła lub nieposortowana siatka godzin UTC")
    if any(base[c].null_count() for c in ["typ_stacji", "typ_obszaru"]):
        raise ValueError("Brak metadanych stacji")
    spans = base.group_by("kod_stacji").agg(
        pl.col("data_czas_utc").min().alias("start"),
        pl.col("data_czas_utc").max().alias("end"),
    )
    if spans["start"].n_unique() != 1 or spans["end"].n_unique() != 1:
        raise ValueError("Stacje mają różne zakresy siatki godzin")


def add_features(base):
    validate_base(base)
    return base.with_columns(*calendar_features(), *pm25_features(), *weather_features())


def main():
    measurements = pl.read_parquet(SILVER_DIR / "pomiary_2024.parquet")
    weather = pl.read_parquet(SILVER_DIR / "openmeteo_2024.parquet")
    stations = pl.read_parquet(BRONZE_DIR / "stacje.parquet")

    features = add_features(build_base(measurements, weather, stations))
    features.write_parquet(SILVER_DIR / "pm25_pogoda_2024.parquet")

    print("Cechy:", features.shape)
    return features


if __name__ == "__main__":
    main()
