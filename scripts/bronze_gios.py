import polars as pl

def xlsx_to_bronze(archive, name):
    raw = pl.read_excel(
        archive.read(name),
        has_header=False,
        infer_schema_length=0,
    )
    header = raw.head(6)
    data = raw.slice(6)

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
        pl.col('data_czas').str.to_datetime('%Y-%m-%d %H:%M:%S'),
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