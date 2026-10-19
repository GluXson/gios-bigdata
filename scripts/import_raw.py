import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = ROOT / "notebooks" / "01_raw_import.ipynb"


def markdown_cell(text):
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": text,
    }


def code_cell(code):
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": code,
    }


def main():
    if NOTEBOOK_PATH.exists():
        print(
            f"Notebook już istnieje — nie nadpisuję:\n{NOTEBOOK_PATH}"
        )
        return

    cells = [
        markdown_cell(
            "# GIOŚ — import danych do warstwy RAW\n\n"
            "## Cel\n"
            "Pobranie oryginalnych plików źródłowych GIOŚ "
            "i zapisanie ich lokalnie wraz z metadanymi.\n\n"
            "Na tym etapie nie rozpakowujemy archiwów, "
            "nie czyścimy danych i nie tworzymy warstwy bronze.\n\n"
            "Kod pobierania znajduje się w "
            "`scripts/download_raw.py`."
        ),
        markdown_cell(
            "## 1. Instalacja zależności\n\n"
            "Komórka instaluje zależności w środowisku "
            "aktualnie wybranego kernela."
        ),
        code_cell(
            "%pip install requests duckdb polars pyarrow scikit-learn"
        ),
        markdown_cell(
            "## 2. Lokalizacja projektu\n\n"
            "Notebook można uruchomić z katalogu głównego projektu "
            "albo z katalogu `notebooks`."
        ),
        code_cell(
            "from pathlib import Path\n"
            "import json\n"
            "import runpy\n"
            "import sys\n"
            "import platform\n"
            "from importlib.metadata import version\n\n"
            "cwd = Path.cwd().resolve()\n"
            "ROOT = cwd.parent if cwd.name == 'notebooks' else cwd\n"
            "CONFIG_PATH = ROOT / 'config' / 'sources.json'\n"
            "RAW_DIR = ROOT / 'data' / 'raw'\n"
            "SCRIPT_PATH = ROOT / 'scripts' / 'download_raw.py'\n\n"
            "if not CONFIG_PATH.is_file() or not SCRIPT_PATH.is_file():\n"
            "    raise FileNotFoundError(\n"
            "        'Uruchom notebook z katalogu projektu lub notebooks.'\n"
            "    )\n\n"
            "RAW_DIR.mkdir(parents=True, exist_ok=True)\n\n"
            "print('Projekt:', ROOT)\n"
            "print('Interpreter:', sys.executable)\n"
            "print('Python:', platform.python_version())\n\n"
            "for package in [\n"
            "    'requests', 'duckdb', 'polars', 'pyarrow', 'scikit-learn'\n"
            "]:\n"
            "    print(f'{package}: {version(package)}')"
        ),
        markdown_cell(
            "## 3. Konfiguracja źródeł\n\n"
            "Przed importem uzupełnij `config/sources.json` "
            "bezpośrednimi adresami plików źródłowych.\n\n"
            "Źródło danych: Główny Inspektorat Ochrony Środowiska.\n\n"
            "Bank danych: https://powietrze.gios.gov.pl/pjp/archives"
        ),
        code_cell(
            "config = json.loads(\n"
            "    CONFIG_PATH.read_text(encoding='utf-8-sig')\n"
            ")\n\n"
            "print(json.dumps(config, ensure_ascii=False, indent=2))"
        ),
        markdown_cell(
            "## 4. Import do RAW\n\n"
            "Uruchamiamy funkcję importu z modułu pomocniczego.\n\n"
            "Dla każdego pliku zapisujemy oryginalną zawartość "
            "oraz metadane: URL, datę pobrania, rozmiar i SHA-256.\n\n"
            "Poprawne, wcześniej pobrane pliki są pomijane."
        ),
        code_cell(
            "import_module = runpy.run_path(str(SCRIPT_PATH))\n"
            "import_module['main']()"
        ),
        markdown_cell(
            "## 5. Podsumowanie lokalnych danych RAW\n\n"
            "Zestawienie poniżej obejmuje pliki znajdujące się "
            "w warstwie RAW i posiadające plik metadanych. "
            "Nie oznacza liczby rekordów pomiarowych — "
            "ich liczenie będzie częścią kolejnego etapu."
        ),
        code_cell(
            "records = []\n\n"
            "for metadata_path in sorted(\n"
            "    RAW_DIR.rglob('*.metadata.json')\n"
            "):\n"
            "    metadata = json.loads(\n"
            "        metadata_path.read_text(encoding='utf-8')\n"
            "    )\n"
            "    data_path = metadata_path.parent / metadata['filename']\n"
            "    if data_path.is_file():\n"
            "        records.append(metadata)\n\n"
            "total_bytes = sum(item['size_bytes'] for item in records)\n\n"
            "print('Liczba plików:', len(records))\n"
            "print(f'Łączny rozmiar: {total_bytes / 1_000_000:.2f} MB')\n\n"
            "for item in records:\n"
            "    print()\n"
            "    print('Plik:', item['filename'])\n"
            "    print('Rok:', item['year'])\n"
            "    print('Bajty:', item['size_bytes'])\n"
            "    print('Pobrano UTC:', item['downloaded_at_utc'])\n"
            "    print('SHA-256:', item['sha256'])"
        ),
    ]

    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python (GIOS Big Data)",
                "language": "python",
                "name": "gios-bigdata",
            },
            "language_info": {
                "name": "python",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 4,
    }

    NOTEBOOK_PATH.parent.mkdir(parents=True, exist_ok=True)

    NOTEBOOK_PATH.write_text(
        json.dumps(notebook, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Utworzono notebook:\n{NOTEBOOK_PATH}")
    print("Otwórz go w JupyterLab i wykonuj komórki od góry.")


if __name__ == "__main__":
    main()