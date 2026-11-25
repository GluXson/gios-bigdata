# GIOŚ — projekt Big Data

Potok pomiarów jakości powietrza GIOŚ i godzinowej pogody Open-Meteo dla **2023–2025**. Cel: analiza i model godzinowego PM2.5. Zasady projektu: prostota, intuicyjność, KISS/YAGNI i jawne reguły przetwarzania.

## Uruchomienie

Korzystaj z istniejącego środowiska `.venv` i jego kernela w Jupyter. Notebooki nie instalują bibliotek. Zależności są w `requirements.txt`; ich instalację lub zmianę wykonuj we własnym terminalu.

```powershell
.venv\Scripts\python.exe -m jupyterlab
```

Wykonuj notebooki w kolejności:

1. `01_raw_import.ipynb` — pobiera archiwa GIOŚ i pogodę.
2. `02_bronze_gios.ipynb` — konwertuje wszystkie roczniki do Parquet.
3. `03_bronze_openmeteo.ipynb` — konwertuje pogodę.
4. `04_profilowanie.ipynb` — profiluje cały zakres w DuckDB. Zachowane opisy historycznych obserwacji 2024 są oznaczone, a aktualne wyniki pochodzą z zapytań.
5. `05_silver.ipynb` — czyści pomiary i pogodę.
6. `06_inz_cech.ipynb` — łączy dane i liczy cechy ciągle przez trzy lata; zapisuje też pokrycie i wycinek dla benchmarku.
7. `07_partycjonowanie_benchmark.ipynb` — niezależny eksperyment na wycinku 2024.

Sam potok bez profilowania i benchmarku można wykonać z katalogu projektu:

```powershell
.venv\Scripts\python.exe -m scripts.download_raw
.venv\Scripts\python.exe -m scripts.download_openmeteo
.venv\Scripts\python.exe -m scripts.bronze_gios
.venv\Scripts\python.exe -m scripts.bronze_openmeteo
.venv\Scripts\python.exe -m scripts.silver_clean
.venv\Scripts\python.exe -m scripts.silver_features
```

Testy bez instalowania zależności:

```powershell
.venv\Scripts\python.exe -m unittest discover -s tests -v
```

## Konfiguracja i pliki

`config/sources.json` jest jedyną listą lat i adresów archiwów. `scripts/pipeline_config.py` wylicza zakres i wspólne ścieżki oraz zawiera jawne korekty kodów stacji i uzupełnienie metadanych. Kalendarz świąt w `silver_features.py` obejmuje zakres projektu 2023–2025 oraz lokalny Nowy Rok 2026; rozszerzenie projektu poza ten zakres wymaga jego aktualizacji.

| Warstwa | Pliki |
|---|---|
| RAW | `data/raw/<rok>/pomiary_<rok>.zip`, `data/raw/stacje/metadane_stacji.xlsx`, `data/raw/openmeteo/<rok>/<stacja>.json` |
| BRONZE | `data/bronze/pomiary_2023_2025.parquet`, `openmeteo_2023_2025.parquet`, `stacje.parquet`, `stanowiska.parquet`; pomocniczo roczne Parquet pomiarów |
| SILVER | `data/silver/pomiary_2023_2025.parquet`, `openmeteo_2023_2025.parquet`, `pm25_pogoda_2023_2025.parquet` |
| Pokrycie | `data/silver/pokrycie_2023_2025.csv`: stacja, rok UTC, godziny siatki, dostępne oczyszczone pomiary PM2.5 i procent pokrycia |
| Benchmark | `data/silver/pm25_pogoda_2024.parquet` — wycinek gotowych cech z pełnego zakresu |

Pliki danych są poza Gitem. Pobieranie zachowuje oryginały z URL, datą pobrania, rozmiarem i SHA-256. Poprawne istniejące pliki są wykorzystywane ponownie. Pogoda jest sprawdzana również pod kątem pełnej, uporządkowanej siatki godzin roku, w tym 29 lutego.

## Jawne reguły i ograniczenia

- Zbiór stacji jest sumą stacji PM2.5 ze wszystkich roczników, nie częścią wspólną. Dla każdej lokalizacji pobieramy pełną pogodę 2023–2025. Pogoda sprzed uruchomienia stacji jest reanalizą dla lokalizacji, nie pomiarem działającej stacji.
- Brak PM2.5 pozostaje NULL, również gdy stacja nie występuje w całym roczniku. Raport pokrycia nie utożsamia braku pomiaru z zerowym stężeniem.
- Kluczem cech jest `(kod_stacji, data_czas_utc)`. Pełna siatka zawiera 26 304 godziny na stację. Lagi i okna przechodzą przez granice lat; braki godzin nie są skracane.
- GIOŚ jest w CET, a projekt interpretuje znacznik godzinowy jako koniec okresu: przesunięcie −2 h daje początek godziny UTC. Założenie o końcu okresu wynika z danych. Pierwsza godzina GIOŚ 2023 trafia do 2022-12-31 23:00 UTC i pozostaje poza siatką analizy. Nie pobieramy 2026, więc końcówka 2025 UTC może nie mieć pomiaru PM2.5.
- Bronze zachowuje ułamki sekund. Godzinowe arkusze 2025 mają narastający dryf poniżej minuty (do około 44 s). Silver wyrównuje je do początku godziny przed czyszczeniem i złączeniem. Przesunięcie obejmujące minuty zatrzymuje przetwarzanie zamiast cicho poprawiać dane.
- Import obsługuje dodatkowy wiersz nagłówka NO2 2023. Depozycja jest pomijana we wszystkich latach ze względu na inny format i wielkość fizyczną.
- Kody `SlGoczaUzdro` i `SlRaciborzWP` są łączone z odpowiednikami `MOB` ze słownika; dotychczasowe korekty Brzegu i Szczecina pozostają. Stacja `DsDusznSportMOB` ma osobne uzupełnienie współrzędnych i typów z [karty GIOŚ](https://powietrze.gios.gov.pl/pjp/current/station_details/info/10093); nie utożsamiamy jej ze starszą `DsDusznikMOB`. Uzupełnienie jest stosowane jawnie przy pobieraniu pogody i budowaniu cech, bez zmiany surowego XLSX.
- Metadane są jednym snapshotem, nie historycznym słownikiem dla każdego roku. Zmiany lokalizacji i typu stacji wymagają osobnej oceny.
- Czyszczenie zachowuje reguły projektu: brak pomiaru, duplikat, wartości ujemne, zero pyłu, stałe PM2.5 przez co najmniej 12 godzin i izolowane skoki ponad 50. Luka czasowa przerywa ciąg; filtr może usuwać również rzeczywiste epizody.
- Kalendarz jest lokalny (Europe/Warsaw), oś czasu UTC. [Wigilia jest świętem od 2025](https://www.pip.gov.pl/dla-pracodawcow/pytania-i-odpowiedzi/wigilia-dniem-wolnym-od-pracy).

## Benchmark

Benchmark pozostaje niezależny od analizy trzech lat. Czyta wycinek 2024, przygotowany **po** obliczeniu cech z historią grudnia 2023. Wycinek obejmuje sumę stacji z projektu, także z brakującymi pomiarami w 2024. Skalowanie ×1 i ×10 kopiuje rekordy pod nowymi identyfikatorami stacji; nie dodaje lat ani nowych wartości. Zwiększenie liczby stacji nie jest równoważne wydłużeniu ich historii.

Porównuje A: płaski Parquet, B: K grup stacji, C: 3K grup stacji. Warianty mają wspólny writer PyArrow, kompresję Snappy i ustawienia grup wierszy. Mierzy DuckDB `execute + fetchall`, bez przygotowania danych; cache systemu nie jest czyszczony. Tryb `quick` sprawdza schemat i liczby wierszy, `full` również zgodność wszystkich wartości i kluczy. Wyniki zapytań A/B/C są porównywane poza pomiarem czasu.

Dane eksperymentu i cache trafiają do `data/benchmark/` i `data/benchmark_cache/`, raporty do `reports/partition_benchmark/`. Benchmark nie publikuje danych dla dalszej analizy. Stare katalogi partycji 2024 nie są źródłem dla gold.

## Dalsza analiza i model

Czytaj `data/silver/pm25_pogoda_2023_2025.parquet`. Nie przeliczaj lagów po usunięciu braków lub próbkowaniu. W gold wybierz horyzont i cechy, obsługę braków oraz podział czasowy. Dla prognozy 24 h wyklucz lagi 1–3 h i okna kończące się na t−1. Pogoda jest reanalizą, nie prognozą dostępną w chwili predykcji.

Źródła: [GIOŚ — Bank danych pomiarowych](https://powietrze.gios.gov.pl/pjp/archives), [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api), licencja pogody CC BY 4.0.
