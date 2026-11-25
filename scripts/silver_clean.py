
import polars as pl


try:
    from .pipeline_config import BRONZE_DIR, SILVER_DIR, PERIOD, STATION_CODES
except ImportError:
    from pipeline_config import BRONZE_DIR, SILVER_DIR, PERIOD, STATION_CODES

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
    hourly_rows = measurements.filter(pl.col("czas_usredniania") == "1g")
    if hourly_rows.filter(pl.col("data_czas").dt.minute() != 0).height:
        raise ValueError("Godzinowe pomiary mają przesunięcie co najmniej minuty")
    # Arkusze 2025 mają narastający dryf sekund (do 44 s); bronze zachowuje oryginał.
    measurements = measurements.with_columns(
        pl.when(pl.col("czas_usredniania") == "1g")
        .then(pl.col("data_czas").dt.truncate("1h"))
        .otherwise(pl.col("data_czas")).alias("data_czas")
    )
    pm25 = (pl.col("wskaznik") == "PM2.5") & (pl.col("czas_usredniania") == "1g")
    value = pl.col("wartosc")

    # Ciągi i piki liczone na pełnej siatce godzin, więc brak pomiaru przerywa ciąg.
    previous_time = pl.col("data_czas").diff().over("kod_stanowiska")
    adjacent = previous_time == pl.duration(hours=1)
    new_run = value.ne_missing(value.shift(1).over("kod_stanowiska")) | ~adjacent.fill_null(False)
    run = pl.col("_run")
    run_hours = pl.len().over("kod_stanowiska", run)
    rise = pl.when(adjacent).then(value - value.shift(1).over("kod_stanowiska"))
    fall = pl.when(pl.col("data_czas").shift(-1).over("kod_stanowiska") - pl.col("data_czas") == pl.duration(hours=1)).then(value - value.shift(-1).over("kod_stanowiska"))

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
        .with_columns(new_run.alias("_new_run"))
        .with_columns(pl.col("_new_run").cum_sum().over("kod_stanowiska").alias("_run"))
        .with_columns(reason.alias("powod_usuniecia"))
        .drop("_run", "_new_run")
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
        pl.col("kod_stacji").str.strip_chars().replace(STATION_CODES),
        pl.when(pl.col("wind_speed_10m") == 0)
        .then(None)
        .otherwise(pl.col("wind_direction_10m"))
        .alias("wind_direction_10m"),
    )


def main():
    flagged = flag_measurements(pl.read_parquet(BRONZE_DIR / f"pomiary_{PERIOD}.parquet"))
    measurements = clean_measurements(flagged)
    weather = clean_weather(pl.read_parquet(BRONZE_DIR / f"openmeteo_{PERIOD}.parquet"))

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    measurements.write_parquet(SILVER_DIR / f"pomiary_{PERIOD}.parquet")
    weather.write_parquet(SILVER_DIR / f"openmeteo_{PERIOD}.parquet")

    print(flagged.group_by("powod_usuniecia").len().sort("len", descending=True))
    print("Pomiary:", flagged.height, "->", measurements.height)

    return flagged, measurements, weather


if __name__ == "__main__":
    main()
