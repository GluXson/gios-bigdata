import json
import runpy
import time
import zipfile
from pathlib import Path

import polars as pl


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
POMIARY_ZIP = RAW_DIR / "2024" / "pomiary_2024.zip"
STACJE_XLSX = RAW_DIR / "stacje" / "metadane_stacji.xlsx"

YEAR = 2024
PM25_FILE = f"{YEAR}_PM25_1g.xlsx"
DATASET = f"openmeteo/{YEAR}"

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

download_raw = runpy.run_path(str(Path(__file__).with_name("download_raw.py")))


def pm25_station_codes():
    with zipfile.ZipFile(POMIARY_ZIP) as archive:
        header = pl.read_excel(
            archive.read(PM25_FILE),
            has_header=False,
            infer_schema_length=0,
        ).head(6)

    return sorted(set(header.row(1)[1:]))


def station_coordinates(codes):
    stations = pl.read_excel(
        STACJE_XLSX,
        sheet_name="STACJE",
        infer_schema_length=0,
    )

    coordinates = (
        stations
        .filter(pl.col("Kod stacji").is_in(codes))
        .select(
            pl.col("Kod stacji").alias("kod_stacji"),
            pl.col("WGS84 φ N").alias("szerokosc"),
            pl.col("WGS84 λ E").alias("dlugosc"),
        )
        .unique("kod_stacji", keep="first", maintain_order=True)
        .sort("kod_stacji")
    )

    missing = set(codes) - set(coordinates["kod_stacji"])

    if missing:
        raise ValueError(f"Brak współrzędnych dla stacji: {sorted(missing)}")

    return coordinates


def build_url(latitude, longitude):
    return (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={latitude}&longitude={longitude}"
        f"&start_date={YEAR}-01-01&end_date={YEAR}-12-31"
        f"&hourly={','.join(HOURLY)}"
        "&timezone=GMT"
    )


def weather_error(path):
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

    lengths = {len(hourly[name]) for name in ["time", *HOURLY]}

    if len(lengths) != 1 or 0 in lengths:
        return "puste albo nierównej długości listy w 'hourly'"

    return None


def import_weather(session, source):
    target = RAW_DIR / DATASET / source["filename"]
    metadata_path = target.with_name(target.name + ".metadata.json")

    for attempt in range(1, ATTEMPTS + 1):
        already_downloaded = target.exists()
        metadata = download_raw["import_source"](session, source)
        error = weather_error(target)

        if not already_downloaded:
            time.sleep(PAUSE_SECONDS)

        if error is None:
            return metadata

        download_raw["logger"].warning(
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
    coordinates = station_coordinates(pm25_station_codes())
    results = []

    with download_raw["create_session"]() as session:
        for row in coordinates.iter_rows(named=True):
            source = {
                "dataset": DATASET,
                "provider": "Open-Meteo",
                "url": build_url(row["szerokosc"], row["dlugosc"]),
                "filename": f"{row['kod_stacji']}.json",
            }

            results.append(import_weather(session, source))

    print(f"Stacje z pogodą: {len(results)}")

    return results


if __name__ == "__main__":
    main()
