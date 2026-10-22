# GIOŚ — projekt Big Data

Potok danych dla pomiarów jakości powietrza GIOŚ z 2024 r. i godzinowej pogody z Open-Meteo. Cel: model przewidujący godzinowe stężenie PM2.5 na podstawie pogody.

## Stack technologiczny

- Python — język projektu.
- JupyterLab / Jupyter Notebook — uruchamianie i dokumentowanie potoku danych.
- Requests — pobieranie plików źródłowych.
- DuckDB — analizy SQL.
- Polars — przetwarzanie i transformacje danych.
- PyArrow — obsługa formatu Parquet.
- Scikit-learn — przygotowanie i ocena modeli predykcyjnych.
- venv — izolowane środowisko Python.

## Etapy

| Warstwa | Zawartość | Stan |
|---|---|---|
| RAW | Oryginalne pliki GIOŚ i Open-Meteo z metadanymi pobrania (URL, data, rozmiar, SHA-256) | gotowe |
| BRONZE | Te same dane w Parquet: format długi (1 wiersz = 1 pomiar), ujednolicone nazwy i typy, bez czyszczenia | gotowe |
| SILVER | Oczyszczone PM2.5 złączone z pogodą, cechy pochodne, partycje | Milestone 2 |
| GOLD | Próbka cech pod model, trening i ewaluacja | Milestone 3 |

## Struktura folderów

```text
gios-bigdata/
├── config/
│   └── sources.json              # Adresy plików GIOŚ (pomiary, metadane stacji)
├── data/                         # Nie jest w repozytorium — powstaje po uruchomieniu notebooków
│   ├── raw/                      # Oryginalne pliki i metadane pobrań
│   │   ├── 2024/                 #   pomiary GIOŚ (ZIP z XLSX)
│   │   ├── stacje/               #   metadane stacji GIOŚ (XLSX)
│   │   └── openmeteo/2024/       #   pogoda Open-Meteo (JSON, jeden na stację)
│   └── bronze/                   # pomiary_2024, stacje, stanowiska, openmeteo_2024 (Parquet)
├── notebooks/
│   ├── 01_raw_import.ipynb       # Pobieranie wszystkich źródeł do RAW
│   ├── 02_bronze_gios.ipynb      # RAW → bronze: pomiary, stacje, stanowiska
│   └── 02_bronze_openmeteo.ipynb # RAW → bronze: pogoda
├── scripts/
│   ├── download_raw.py           # Pobieranie plików do RAW
│   ├── download_openmeteo.py     # Pobieranie pogody dla stacji PM2.5
│   ├── bronze_gios.py            # Konwersja GIOŚ do bronze
│   └── bronze_openmeteo.py       # Konwersja pogody do bronze
├── .gitignore
├── README.md
└── requirements.txt
```

## Uruchomienie

Polecenia dla Windows CMD, wykonywane z katalogu projektu.

### 1. Utworzenie środowiska i instalacja zależności

```bat
py -3 -m venv .venv
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\python.exe -m pip install -r requirements.txt
```

### 2. Rejestracja kernela Jupyter

```bat
.venv\Scripts\python.exe -m ipykernel install --user --name gios-bigdata --display-name "Python (GIOS Big Data)"
```

W notebookach wybieraj kernel `Python (GIOS Big Data)`. Domyślny kernel `Python 3` może uruchomić systemowy Python zamiast `.venv`.

### 3. Źródła danych

Adresy plików GIOŚ są w `config/sources.json`. Wpisy z polem `year` trafiają do `data/raw/<rok>/`, a wpisy z polem `dataset` do `data/raw/<dataset>/`. Adresy pogody Open-Meteo skrypt buduje sam ze współrzędnych stacji.

### 4. Uruchomienie notebooków

```bat
.venv\Scripts\python.exe -m jupyterlab
```

Wykonuj komórki od góry, w tej kolejności:

1. `01_raw_import.ipynb` — pobiera pomiary i metadane stacji GIOŚ, potem pogodę dla 96 stacji PM2.5 (kilka minut, 2 s przerwy między zapytaniami).
2. `02_bronze_gios.ipynb` — zapisuje `pomiary_2024.parquet`, `stacje.parquet` i `stanowiska.parquet`.
3. `02_bronze_openmeteo.ipynb` — zapisuje `openmeteo_2024.parquet`.

Ponowne uruchomienie notebooka 01 nie pobiera plików, które już są i mają zgodną sumę SHA-256.
