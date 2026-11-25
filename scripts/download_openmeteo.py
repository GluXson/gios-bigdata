import json
from datetime import datetime, timedelta
import time
import zipfile
from pathlib import Path
from urllib.parse import urlsplit, parse_qsl

import polars as pl


try:
    from . import download_raw
    from .pipeline_config import RAW_DIR, YEARS, STATION_CODES, EXTRA_STATIONS
except ImportError:
    import download_raw
    from pipeline_config import RAW_DIR, YEARS, STATION_CODES, EXTRA_STATIONS

STACJE_XLSX = RAW_DIR / "stacje" / "metadane_stacji.xlsx"

HOURLY = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
]

PAUSE_SECONDS = 2
ATTEMPTS = 3




def pm25_station_codes(year):
    with zipfile.ZipFile(RAW_DIR / str(year) / f"pomiary_{year}.zip") as archive:
        header = pl.read_excel(
            archive.read(f"{year}_PM25_1g.xlsx"),
            has_header=False,
            infer_schema_length=0,
        ).head(6)

    return sorted({STATION_CODES.get(code.strip(), code.strip()) for code in header.row(1)[1:]})


def station_coordinates(codes):
    stations = pl.read_excel(
        STACJE_XLSX,
        sheet_name="STACJE",
        infer_schema_length=0,
    )

    coordinates = (
        stations
        .filter(pl.col("Kod stacji").str.strip_chars().is_in(codes))
        .select(
            pl.col("Kod stacji").str.strip_chars().alias("kod_stacji"),
            pl.col("WGS84 φ N").cast(pl.Float64).alias("szerokosc"),
            pl.col("WGS84 λ E").cast(pl.Float64).alias("dlugosc"),
        )
        .unique("kod_stacji", keep="first", maintain_order=True)
        .sort("kod_stacji")
    )

    extra = pl.DataFrame(EXTRA_STATIONS).select("kod_stacji", "szerokosc", "dlugosc")
    coordinates = (
        pl.concat([coordinates, extra.filter(pl.col("kod_stacji").is_in(codes))])
        .unique("kod_stacji", keep="first")
        .sort("kod_stacji")
    )
    if coordinates.filter(~pl.col("szerokosc").is_between(-90, 90) | ~pl.col("dlugosc").is_between(-180, 180)).height:
        raise ValueError("Nieprawidłowe współrzędne stacji")
    missing = set(codes) - set(coordinates["kod_stacji"])

    if missing:
        raise ValueError(f"Brak współrzędnych dla stacji: {sorted(missing)}")

    return coordinates


def build_url(latitude, longitude, year):
    return (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={latitude}&longitude={longitude}"
        f"&start_date={year}-01-01&end_date={year}-12-31"
        f"&hourly={','.join(HOURLY)}"
        "&timezone=GMT"
    )


def weather_error(path, year):
    # Open-Meteo potrafi zwrócić komunikat błędu z kodem 200,
    # np. "Unexpected error while streaming data: timeoutReached".
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return "plik nie jest poprawnym JSON-em"

    hourly = payload.get("hourly") if isinstance(payload, dict) else None

    if not isinstance(hourly, dict):
        return "brak sekcji 'hourly'"

    missing = [name for name in ["time", *HOURLY] if name not in hourly]

    if missing:
        return f"brak zmiennych w 'hourly': {missing}"

    if any(not isinstance(hourly[name], list) for name in ["time", *HOURLY]):
        return "zmienne hourly muszą być listami"

    lengths = {len(hourly[name]) for name in ["time", *HOURLY]}

    if len(lengths) != 1 or 0 in lengths:
        return "puste albo nierównej długości listy w 'hourly'"

    start = datetime(year, 1, 1)
    hours = int((datetime(year + 1, 1, 1) - start).total_seconds() / 3600)
    expected = [(start + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M") for h in range(hours)]
    if hourly["time"] != expected:
        return f"niepełna lub nieuporządkowana siatka godzin {year}"
    return None


def import_weather(session, source):
    target = RAW_DIR / source["dataset"] / source["filename"]
    metadata_path = target.with_name(target.name + ".metadata.json")

    if target.exists() and metadata_path.exists():
        previous_url = json.loads(metadata_path.read_text(encoding="utf-8"))["source_url"]
        def normalized_url(url):
            parts = urlsplit(url)
            params = dict(parse_qsl(parts.query))
            for key in ("latitude", "longitude"):
                params[key] = float(params[key])
            return parts.scheme, parts.netloc, parts.path, params
        if normalized_url(previous_url) == normalized_url(source["url"]):
            source = {**source, "url": previous_url}

    for attempt in range(1, ATTEMPTS + 1):
        already_downloaded = target.exists()
        metadata = download_raw.import_source(session, source)
        error = weather_error(target, source["year"])

        if not already_downloaded:
            time.sleep(PAUSE_SECONDS)

        if error is None:
            return metadata

        download_raw.logger.warning(
            "Niepoprawna odpowiedź Open-Meteo (%s): %s — usuwam, próba %d/%d",
            source["filename"],
            error,
            attempt,
            ATTEMPTS,
        )
        target.unlink()
        metadata_path.unlink(missing_ok=True)

    raise RuntimeError(
        f"Nie udało się pobrać poprawnej pogody: {source['filename']}"
    )


def main():
    codes = sorted(set().union(*(pm25_station_codes(year) for year in YEARS)))
    coordinates = station_coordinates(codes)
    results = []

    with download_raw.create_session() as session:
        for year in YEARS:
            for row in coordinates.iter_rows(named=True):
                source = {
                    "dataset": f"openmeteo/{year}",
                    "year": year,
                    "provider": "Open-Meteo",
                    "url": build_url(row["szerokosc"], row["dlugosc"], year),
                    "filename": f"{row['kod_stacji']}.json",
                }
                results.append(import_weather(session, source))

    print(f"Stacje z pogodą: {len(results)}")

    return results


if __name__ == "__main__":
    main()
