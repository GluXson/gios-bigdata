from pathlib import Path

import polars as pl


ROOT = Path(__file__).resolve().parents[1]
BRONZE_DIR = ROOT / "data" / "bronze"
SILVER_DIR = ROOT / "data" / "silver"

# Kody z plików pomiarów, które w słowniku stacji mają inny kod (anomalia 8.10).
STATION_CODES = {
    "ZpSzczLaczna": "ZpSzczLacz04",
    "OpBrzegPoprzMOB": "OpBrzegPoprz",
}

# GIOŚ: UTC+1 i znacznik końca godziny; Open-Meteo: UTC i znacznik początku godziny (8.9).
GIOS_TO_UTC = pl.duration(hours=2)

DUST = ["PM2.5", "PM10"]
STALE_HOURS = 12
PEAK_JUMP = 50

REASONS = [
    "brak pomiaru",
    "duplikat stanowiska",
    "wartość ujemna",
    "zero pyłu",
    "PM2.5 stałe >= 12 h",
    "PM2.5 jednogodzinny pik",
]


def flag_measurements(measurements):
    """Dodaje kolumnę `powod_usuniecia` (NULL = wiersz zostaje)."""
    pm25 = (pl.col("wskaznik") == "PM2.5") & (pl.col("czas_usredniania") == "1g")
    value = pl.col("wartosc")

    # Ciągi i piki liczone na pełnej siatce godzin, więc brak pomiaru przerywa ciąg.
    run = value.rle_id().over("kod_stanowiska")
    run_hours = pl.len().over("kod_stanowiska", run)
    rise = value - value.shift(1).over("kod_stanowiska")
    fall = value - value.shift(-1).over("kod_stanowiska")

    reason = (
        pl.when(value.is_null()).then(pl.lit(REASONS[0]))
        .when(~pl.struct("kod_stanowiska", "data_czas").is_first_distinct()).then(pl.lit(REASONS[1]))
        .when(value < 0).then(pl.lit(REASONS[2]))
        .when(pl.col("wskaznik").is_in(DUST) & (value == 0)).then(pl.lit(REASONS[3]))
        .when(pm25 & (run_hours >= STALE_HOURS)).then(pl.lit(REASONS[4]))
        .when(pm25 & (rise > PEAK_JUMP) & (fall > PEAK_JUMP)).then(pl.lit(REASONS[5]))
    )

    return (
        measurements
        .with_columns(
            pl.col("kod_stacji").str.strip_chars().replace(STATION_CODES),
            pl.col("kod_stanowiska").str.strip_chars(),
        )
        .sort("kod_stanowiska", "data_czas")
        .with_columns(reason.alias("powod_usuniecia"))
    )


def clean_measurements(flagged):
    return (
        flagged
        .filter(pl.col("powod_usuniecia").is_null())
        .drop("powod_usuniecia")
        .with_columns(
            # Pomiary dobowe mają znacznik doby (00:00), więc ich nie przesuwamy.
            pl.when(pl.col("czas_usredniania") == "1g")
            .then(pl.col("data_czas") - GIOS_TO_UTC)
            .otherwise(pl.col("data_czas"))
            .alias("data_czas_utc")
        )
    )


def clean_weather(weather):
    # Przy ciszy Open-Meteo wpisuje kierunek 270°, a kierunek jest wtedy nieokreślony (8.14).
    return weather.with_columns(
        pl.col("kod_stacji").str.strip_chars(),
        pl.when(pl.col("wind_speed_10m") == 0)
        .then(None)
        .otherwise(pl.col("wind_direction_10m"))
        .alias("wind_direction_10m"),
    )


def main():
    flagged = flag_measurements(pl.read_parquet(BRONZE_DIR / "pomiary_2024.parquet"))
    measurements = clean_measurements(flagged)
    weather = clean_weather(pl.read_parquet(BRONZE_DIR / "openmeteo_2024.parquet"))

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    measurements.write_parquet(SILVER_DIR / "pomiary_2024.parquet")
    weather.write_parquet(SILVER_DIR / "openmeteo_2024.parquet")

    print(flagged.group_by("powod_usuniecia").len().sort("len", descending=True))
    print("Pomiary:", flagged.height, "->", measurements.height)

    return flagged, measurements, weather


if __name__ == "__main__":
    main()
