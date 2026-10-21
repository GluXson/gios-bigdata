# GIOŚ — projekt Big Data

## Stack technologiczny

- Python — język projektu.
- JupyterLab / Jupyter Notebook — uruchamianie i dokumentowanie potoku danych.
- Requests — pobieranie plików źródłowych.
- DuckDB — analizy SQL.
- Polars — przetwarzanie i transformacje danych.
- PyArrow — obsługa formatu Parquet.
- Scikit-learn — przygotowanie i ocena modeli predykcyjnych.
- venv — izolowane środowisko Python.

Aktualnie zaimplementowany etap: **RAW**. Biblioteki analityczne będą wykorzystywane w kolejnych etapach.

## Struktura folderów

```text
gios-bigdata/
├── config/
│   └── sources.json              # Adresy plików GIOŚ (pomiary, metadane stacji)
├── data/
│   ├── raw/                      # Oryginalne pliki i metadane pobrań
│   │   ├── 2024/                 #   pomiary GIOŚ (ZIP z XLSX)
│   │   ├── stacje/               #   metadane stacji GIOŚ (XLSX)
│   │   └── openmeteo/2024/       #   pogoda Open-Meteo (JSON, jeden na stację)
│   └── bronze/                   # Tabele Parquet po ujednoliceniu kształtu i typów
├── notebooks/
│   ├── 01_raw_import.ipynb       # Pobieranie wszystkich źródeł do RAW
│   ├── 02_bronze_gios.ipynb      # RAW → bronze: pomiary, stacje, stanowiska
│   └── 02_bronze_openmeteo.ipynb # RAW → bronze: pogoda
├── scripts/
│   ├── import_raw.py             # Generator notebooka 01 (nieaktualny)
│   ├── download_raw.py           # Pobieranie plików do RAW
│   ├── download_openmeteo.py     # Pobieranie pogody dla stacji PM2.5
│   ├── bronze_gioś.py            # Konwersja GIOŚ do bronze
│   └── bronze_openmeteo.py       # Konwersja pogody do bronze
├── .gitignore
├── README.md
└── requirements.txt
```

## Uruchomienie

Polecenia dla Windows CMD.

### 1. Przejście do projektu

```bat
cd /d "D:\Projekty\gios-bigdata"
```

### 2. Utworzenie środowiska i instalacja zależności

```bat
py -3 -m venv .venv
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\python.exe -m pip install -r requirements.txt
```

### 3. Rejestracja kernela Jupyter

```bat
.venv\Scripts\python.exe -m ipykernel install --user --name gios-bigdata --display-name "Python (GIOS Big Data)"
```

### 4. Konfiguracja źródeł

W pliku `config/sources.json` uzupełnij bezpośrednie adresy plików:

```json
{
  "sources": [
    {
      "year": 2024,
      "url": "BEZPOSREDNI_ADRES_PLIKU_ZIP",
      "filename": "pomiary_2024.zip"
    }
  ]
}
```

### 5. Wygenerowanie notebooka

```bat
.venv\Scripts\python.exe scripts\import_raw.py
```

Generator utworzy `notebooks/01_raw_import.ipynb`.
Istniejący notebook nie zostanie nadpisany.

### 6. Uruchomienie JupyterLab

```bat
.venv\Scripts\python.exe -m jupyterlab
```

Otwórz `notebooks/01_raw_import.ipynb`, wybierz kernel
`Python (GIOS Big Data)` i wykonuj komórki od góry.

Pobrane pliki i ich metadane zostaną zapisane w `data/raw/<rok>/`.
Archiwa pozostaną nierozpakowane, bez transformacji danych.

Przy kolejnych uruchomieniach wystarczy przejść do katalogu projektu
i uruchomić JupyterLab — ponowna instalacja środowiska nie jest potrzebna.
