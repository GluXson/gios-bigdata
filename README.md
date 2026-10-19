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
│   └── sources.json              # Adresy źródeł, lata i nazwy plików
├── data/
│   └── raw/                      # Oryginalne pliki i metadane pobrań
├── notebooks/
│   └── 01_raw_import.ipynb        # Notebook tworzony przez generator
├── scripts/
│   ├── import_raw.py             # Generator notebooka
│   └── download_raw.py           # Pobieranie danych do RAW
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
