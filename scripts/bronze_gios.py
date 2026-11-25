import zipfile

import polars as pl

try:
    from .pipeline_config import ROOT, RAW_DIR, BRONZE_DIR, YEARS, PERIOD
except ImportError:
    from pipeline_config import ROOT, RAW_DIR, BRONZE_DIR, YEARS, PERIOD


def xlsx_to_bronze(archive, name):
    raw = pl.read_excel(
        archive.read(name),
        has_header=False,
        infer_schema_length=0,
    )
    # NO2 2023 ma dodatkowy wiersz z rokiem przed kodami stanowisk.
    extra_year = raw["column_0"][6] == "Czas pomiaru"
    header = pl.concat([raw.head(5), raw.slice(6, 1)]) if extra_year else raw.head(6)
    data = raw.slice(7 if extra_year else 6)

    columns_info = (
        header
        .drop('column_0')
        .transpose(include_header=True, header_name='kolumna')
    )
    columns_info.columns = [
        'kolumna', 'nr', 'kod_stacji', 'wskaznik',
        'czas_usredniania', 'jednostka', 'kod_stanowiska',
    ]

    long = (
        data
        .rename({'column_0': 'data_czas'})
        .unpivot(index='data_czas', variable_name='kolumna', value_name='wartosc')
        .join(columns_info, on='kolumna', how='left')
    )

    return long.select(
        'kod_stanowiska',
        'kod_stacji',
        'wskaznik',
        'czas_usredniania',
        'jednostka',
        pl.col('data_czas').str.to_datetime('%Y-%m-%d %H:%M:%S%.f'),
        pl.col('wartosc').cast(pl.Float64),
        pl.lit(name).alias('plik_zrodlowy'),
    )


def to_date(column):
    text = pl.col(column).str.strip_chars()

    return (
        pl.when(text == '')
        .then(None)
        .otherwise(text.str.slice(0, 10))
        .str.to_date('%Y-%m-%d', strict=False)
    )


def stations_to_bronze(path):
    stations = pl.read_excel(path, sheet_name='STACJE', infer_schema_length=0)

    return stations.select(
        pl.col('Kod stacji').alias('kod_stacji'),
        pl.col('Kod międzynarodowy').alias('kod_miedzynarodowy'),
        pl.col('Nazwa stacji').alias('nazwa_stacji'),
        pl.col('Stary Kod stacji \n(o ile inny od aktualnego)').alias('stary_kod_stacji'),
        to_date('Data uruchomienia').alias('data_uruchomienia'),
        to_date('Data zamknięcia').alias('data_zamkniecia'),
        pl.col('Typ stacji').alias('typ_stacji'),
        pl.col('Typ obszaru').alias('typ_obszaru'),
        pl.col('Rodzaj stacji').alias('rodzaj_stacji'),
        pl.col('Województwo').alias('wojewodztwo'),
        pl.col('Miejscowość').alias('miejscowosc'),
        pl.col('Adres').alias('adres'),
        pl.col('WGS84 φ N').cast(pl.Float64).alias('szerokosc'),
        pl.col('WGS84 λ E').cast(pl.Float64).alias('dlugosc'),
    )


def positions_to_bronze(path):
    positions = pl.read_excel(path, sheet_name='STANOWISKA', infer_schema_length=0)

    return positions.select(
        pl.col('Kod stanowiska').alias('kod_stanowiska'),
        pl.col('Kod stacji').alias('kod_stacji'),
        pl.col('Wskaźnik - kod').alias('wskaznik'),
        pl.col('Wskaźnik').alias('wskaznik_nazwa'),
        pl.col('Czas uśredniania').alias('czas_usredniania'),
        pl.col('Typ pomiaru').alias('typ_pomiaru'),
        to_date('Data uruchomienia').alias('data_uruchomienia'),
        to_date('Data zamknięcia').alias('data_zamkniecia'),
        pl.col('Województwo').alias('wojewodztwo'),
        pl.col('Nazwa strefy').alias('strefa'),
    )



def main():
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    paths = []
    for year in YEARS:
        with zipfile.ZipFile(RAW_DIR / str(year) / f"pomiary_{year}.zip") as archive:
            parts = []
            for name in archive.namelist():
                if not name.endswith(".xlsx") or "_Depozycja_" in name:
                    continue
                part = xlsx_to_bronze(archive, name)
                print(name, part.height, flush=True)
                parts.append(part)
            frame = pl.concat(parts)
        path = BRONZE_DIR / f"pomiary_{year}.parquet"
        frame.write_parquet(path)
        paths.append(path)
        del parts, frame
    target = BRONZE_DIR / f"pomiary_{PERIOD}.parquet"
    pl.scan_parquet(paths).sink_parquet(target)
    metadata = RAW_DIR / "stacje/metadane_stacji.xlsx"
    stations_to_bronze(metadata).write_parquet(BRONZE_DIR / "stacje.parquet")
    positions_to_bronze(metadata).write_parquet(BRONZE_DIR / "stanowiska.parquet")
    print("Zapisano:", target.relative_to(ROOT).as_posix())
    return target


if __name__ == "__main__":
    main()
