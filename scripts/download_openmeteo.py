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
            already_downloaded = (
                RAW_DIR / DATASET / source["filename"]
            ).exists()

            results.append(
                download_raw["import_source"](session, source)
            )

            if not already_downloaded:
                time.sleep(PAUSE_SECONDS)

    print(f"Stacje z pogodą: {len(results)}")

    return results


if __name__ == "__main__":
    main()
