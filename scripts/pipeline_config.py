"""Wspólny zakres danych; lata pochodzą wyłącznie z konfiguracji źródeł."""
import json
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCES = json.loads((ROOT / "config/sources.json").read_text(encoding="utf-8-sig"))["sources"]
YEARS = sorted(source["year"] for source in SOURCES if "year" in source)
if not YEARS or YEARS != list(range(YEARS[0], YEARS[-1] + 1)):
    raise ValueError("Lata źródeł muszą być unikalne i kolejne")
PERIOD = f"{YEARS[0]}_{YEARS[-1]}"
START = datetime(YEARS[0], 1, 1)
END = datetime(YEARS[-1] + 1, 1, 1)
HOURS = int((END - START).total_seconds() / 3600)
RAW_DIR = ROOT / "data/raw"
BRONZE_DIR = ROOT / "data/bronze"
SILVER_DIR = ROOT / "data/silver"

# Różne kody tej samej lokalizacji w pomiarach i słowniku GIOŚ.
STATION_CODES = {
    "ZpSzczLaczna": "ZpSzczLacz04",
    "OpBrzegPoprzMOB": "OpBrzegPoprz",
    "SlGoczaUzdro": "SlGoczaUzdroMOB",
    "SlRaciborzWP": "SlRaciborzWPMOB",
}

# Stacja 2025 nieobecna w pobranym słowniku; nie utożsamiamy jej ze starą DsDusznikMOB.
# Źródło: https://powietrze.gios.gov.pl/pjp/current/station_details/info/10093
EXTRA_STATIONS = [{
    "kod_stacji": "DsDusznSportMOB", "typ_stacji": "tło", "typ_obszaru": "miejski",
    "szerokosc": 50.402561, "dlugosc": 16.393311,
}]
