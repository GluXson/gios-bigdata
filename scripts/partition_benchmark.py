"""Zoptymalizowany zapis SILVER i benchmark A/B (+ opcjonalny C).

A: jeden plik; B: Hive po kod_stacji, domyslnie lista WSZYSTKICH plikow.
C opcjonalnie: te same partycje odczytywane alternatywnym readerem (glob/lista).
Zapis w RAM, jeden plik na stacje, Snappy i identyczny writer A/B.
Pomiar execute + fetchall. Cache OS nieczyszczony. Bez obietnicy przyspieszenia.
API notebooka zachowane; logika zapisu jest w tym skrypcie.
"""

from contextlib import contextmanager
from uuid import uuid4
from numbers import Real, Integral
import hashlib
import re
from pathlib import Path

from datetime import datetime, timezone

from time import perf_counter_ns

from importlib.metadata import version, PackageNotFoundError

import csv

import json

import math

import os

import platform

import random

import statistics

import shutil

from tempfile import TemporaryDirectory

import duckdb

import pyarrow as pa



import pyarrow.parquet as pq

import polars as pl

ROOT = Path(__file__).resolve().parents[1]

REQUIRED = {"kod_stacji", "data_czas_utc", "pm25", "godzina", "temperature_2m", "pm25_lag24"}
RESULT_COLUMNS = ["godzina", "srednie_pm25", "srednia_temperatura", "srednie_pm25_lag24", "liczba_godzin", "liczba_pomiarow"]


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"

def save_csv(path, rows):
    rows = list(rows)
    if not rows:
        raise ValueError("Brak wierszy do zapisania w CSV")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def package_version(name):
    # Wykresy są osobnym krokiem; zapis i kontrola danych działają bez matplotlib.
    try:
        return version(name)
    except PackageNotFoundError:
        return None


def save_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2,
                                    allow_nan=False), encoding="utf-8")


def identifier(value):
    return '"' + str(value).replace('"', '""') + '"'


def checked_int(value, name, minimum):
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise TypeError(f"{name} musi byc liczba calkowita")
    if value < minimum:
        raise ValueError(f"{name} musi byc >= {minimum}")
    return int(value)


def fingerprint(path):
    path = Path(path)
    before = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise RuntimeError("Zrodlo zmienilo sie podczas liczenia SHA-256")
    return dict(bytes=after.st_size, mtime_ns=after.st_mtime_ns, sha256=digest.hexdigest())


def check_source(ctx):
    if fingerprint(ctx["source"]) != ctx["source_fingerprint"]:
        raise RuntimeError("Zrodlo zmienilo sie od utworzenia kontekstu")


def parquet_reader(path, partitioned=False, files=None):
    if not partitioned:
        if files is not None:
            raise ValueError("files stosujemy tylko do wariantu partycjonowanego")
        return f"read_parquet({literal(Path(path).as_posix())}, hive_partitioning=false)"
    if files is None:
        argument = literal((Path(path) / "kod_stacji=*" / "*.parquet").as_posix())
    else:
        files = list(files)
        if not files:
            raise ValueError("Pusta lista plikow partycji")
        argument = "[" + ", ".join(literal(Path(p).as_posix()) for p in files) + "]"
    return (f"read_parquet({argument}, hive_partitioning=true, "
            "hive_types={'kod_stacji': 'VARCHAR'})")


def configure_readers(ctx, mode=None):
    mode = ctx.get("reader_mode", "all_files_list") if mode is None else mode
    if mode not in {"glob", "all_files_list"}:
        raise ValueError("reader_mode: glob albo all_files_list")
    files = sorted(ctx["partitioned"].glob("kod_stacji=*/*.parquet"))
    if not files:
        raise FileNotFoundError("Brak plikow partycji")
    ctx["partition_readers"] = {
        "glob": parquet_reader(ctx["partitioned"], True),
        "all_files_list": parquet_reader(ctx["partitioned"], True, files=files),
    }
    ctx["reader_mode"] = mode
    ctx["readers"] = {"A": parquet_reader(ctx["flat"]),
                      "B": ctx["partition_readers"][mode]}
    # Zmiana readera uniewaznia SQL i wyniki przygotowanych scenariuszy.
    for key in ("queries", "expected", "scenarios", "variants"):
        ctx.pop(key, None)
    manifest = dict(reader_mode=mode, discovery_outside_timing=(mode == "all_files_list"),
                    files=[p.as_posix() for p in files])
    save_json(ctx["report"] / "reader_manifest.json", manifest)
    return ctx["readers"]


def create_context(root=ROOT, repeats=20, warmups=3, threads=None,
                   write_statistics=False, row_group_size=122880, seed=42,
                   reader_mode="all_files_list", compare_glob=False):
    repeats = checked_int(repeats, "repeats", 2)
    warmups = checked_int(warmups, "warmups", 0)
    threads = checked_int(min(4, os.cpu_count() or 1) if threads is None else threads,
                          "threads", 1)
    row_group_size = checked_int(row_group_size, "row_group_size", 1)
    seed = checked_int(seed, "seed", 0)
    if not isinstance(write_statistics, bool) or not isinstance(compare_glob, bool):
        raise TypeError("write_statistics i compare_glob musza byc bool")
    if reader_mode not in {"glob", "all_files_list"}:
        raise ValueError("reader_mode: glob albo all_files_list")
    root = Path(root).resolve()
    source = root / "data/silver/pm25_pogoda_2024.parquet"
    if not source.is_file():
        raise FileNotFoundError(f"Najpierw utworz tabele cech: {source}")
    source_fingerprint = fingerprint(source)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ") + "_" + uuid4().hex[:8]
    data = root / "data/benchmark" / stamp
    report = root / "reports/partition_benchmark" / stamp
    con = duckdb.connect()
    try:
        con.execute(f"SET threads = {threads}")
        data.mkdir(parents=True, exist_ok=False)
        report.mkdir(parents=True, exist_ok=False)
        ctx = dict(root=root, source=source, flat=data / "flat.parquet",
                   partitioned=data / "partitioned", report=report, data=data,
                   repeats=repeats, warmups=warmups, threads=threads, seed=seed,
                   row_group_size=row_group_size, write_statistics=write_statistics,
                   reader_mode=reader_mode, compare_glob=compare_glob,
                   source_fingerprint=source_fingerprint, con=con)
        save_json(report / "environment.json", {
            "created_utc": stamp, "source": str(source),
            "source_fingerprint": source_fingerprint,
            "python": platform.python_version(), "platform": platform.platform(),
            "processor": platform.processor(), "logical_cpus": os.cpu_count(),
            "threads": threads, "repeats": repeats, "warmups": warmups, "seed": seed,
            "compression": "snappy", "write_statistics": write_statistics,
            "row_group_size_limit": row_group_size,
            "writer": "pyarrow.parquet.write_table, sorted in-memory Arrow table",
            "reader_mode": reader_mode, "compare_glob": compare_glob,
            "cache": "OS cache not cleared; validation warms cache",
            "timing_scope": "execute + fetchall, validation outside timer",
            "file_discovery": "B list built before timing" if reader_mode == "all_files_list"
                              else "B glob resolved by DuckDB",
            "versions": {p: package_version(p) for p in
                         ["duckdb", "pyarrow", "polars", "matplotlib"]},
        })
        return ctx
    except BaseException:
        con.close()
        raise


def write_variants(ctx):
    """Jednakowy writer A/B. Cala tabela cech musi zmiescic sie w RAM."""
    check_source(ctx)
    if ctx["flat"].exists() or ctx["partitioned"].exists():
        raise FileExistsError("Warianty istnieja; utworz nowy kontekst")
    frame = pl.read_parquet(ctx["source"])
    if REQUIRED - set(frame.columns):
        raise ValueError(f"Brak kolumn: {sorted(REQUIRED - set(frame.columns))}")
    if frame.is_empty():
        raise ValueError("Puste zrodlo")
    if frame["kod_stacji"].dtype != pl.String:
        raise TypeError("kod_stacji musi miec typ String")
    if frame["kod_stacji"].null_count() or frame["data_czas_utc"].null_count():
        raise ValueError("NULL w kluczu stacja-czas")
    if frame.select(pl.struct(["kod_stacji", "data_czas_utc"]).is_duplicated().any()).item():
        raise ValueError("Duplikat klucza stacja-czas")
    codes = sorted(frame["kod_stacji"].unique().to_list())
    for station in codes:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", station):
            raise ValueError(f"Kod wymaga jawnego kodowania Hive: {station!r}")
    n = ctx["row_group_size"]
    frame = frame.sort(["kod_stacji", "data_czas_utc"])
    table = frame.to_arrow().combine_chunks()
    counts = frame.group_by("kod_stacji").len().sort("kod_stacji")
    options = dict(compression="snappy", write_statistics=ctx["write_statistics"],
                   row_group_size=n)
    layout = []
    with TemporaryDirectory(dir=ctx["data"], prefix="write-") as temporary:
        temporary = Path(temporary)
        flat = temporary / "flat.parquet"
        partitioned = temporary / "partitioned"
        partitioned.mkdir()
        pq.write_table(table, flat, **options)
        offset = 0
        for station, count in counts.iter_rows():
            folder = partitioned / f"kod_stacji={station}"
            folder.mkdir()
            path = folder / "part-0.parquet"
            part = table.slice(offset, count).drop(["kod_stacji"])
            pq.write_table(part, path, **options)
            with pq.ParquetFile(path) as handle:
                meta = handle.metadata
                if meta.num_rows != count or meta.num_row_groups != (count + n - 1) // n:
                    raise AssertionError(f"Nieoczekiwany uklad grup: {station}")
                layout.append(dict(kod_stacji=station, rows=count, row_groups=meta.num_row_groups,
                                   bytes=path.stat().st_size))
            offset += count
        if offset != table.num_rows:
            raise AssertionError("Nie zapisano wszystkich rekordow")
        check_source(ctx)
        flat.rename(ctx["flat"])
        try:
            partitioned.rename(ctx["partitioned"])
        except BaseException:
            ctx["flat"].unlink(missing_ok=True)
            raise
    configure_readers(ctx)
    save_csv(ctx["report"] / "optimized_partition_layout.csv", layout)
    save_json(ctx["report"] / "optimized_layout.json", {
        "rows": table.num_rows, "columns": table.num_columns,
        "sort_keys": ["kod_stacji", "data_czas_utc"], "partitions": len(codes),
        "files_per_station": 1, "row_group_size_limit": n,
        "compression": "snappy", "write_statistics": ctx["write_statistics"],
        "reader_mode": ctx["reader_mode"], "writer": "pyarrow.parquet.write_table",
    })
    return inspect_storage(ctx)


def inspect_storage(ctx):
    files = sorted(ctx["partitioned"].rglob("*.parquet"))
    if not files:
        raise AssertionError("Brak partycji")
    rows, groups, totals = [], [], []
    for variant, paths in [("A", [ctx["flat"]]), ("B", files)]:
        total_rows = total_bytes = total_groups = 0
        for path in paths:
            if variant == "B" and (path.parent.parent != ctx["partitioned"] or
                                    not path.parent.name.startswith("kod_stacji=")):
                raise AssertionError(f"Nieoczekiwana struktura: {path}")
            with pq.ParquetFile(path) as handle:
                meta = handle.metadata
                size = path.stat().st_size
                total_rows += meta.num_rows
                total_bytes += size
                total_groups += meta.num_row_groups
                if variant == "B":
                    rows.append(dict(partition=path.parent.name, file=path.name,
                                     rows=meta.num_rows, bytes=size, row_groups=meta.num_row_groups))
                for g in range(meta.num_row_groups):
                    rg = meta.row_group(g)
                    groups.append(dict(variant=variant, file=str(path.relative_to(ctx["data"])),
                                       row_group=g, rows=rg.num_rows,
                                       uncompressed_bytes=rg.total_byte_size))
                    for c in range(rg.num_columns):
                        if rg.column(c).compression != "SNAPPY":
                            raise AssertionError(f"Kompresja inna niz Snappy: {path}")
        totals.append(dict(variant=variant, files=len(paths), rows=total_rows,
                           bytes=total_bytes, row_groups=total_groups))
    if totals[0]["rows"] != totals[1]["rows"]:
        raise AssertionError("Rozna liczba rekordow A/B")
    tree = [str(ctx["partitioned"].relative_to(ctx["root"])) + "/"]
    previous = None
    for row in rows:
        if row["partition"] != previous:
            tree.append(f"  {row['partition']}/")
            previous = row["partition"]
        tree.append(f"    {row['file']} ({row['rows']:,} wierszy, {row['bytes']:,} B)")
    tree = "\n".join(tree)
    (ctx["report"] / "directory_tree.txt").write_text(tree, encoding="utf-8")
    save_csv(ctx["report"] / "partition_files.csv", rows)
    save_csv(ctx["report"] / "row_groups.csv", groups)
    save_csv(ctx["report"] / "storage.csv", totals)
    return dict(tree=tree, files=pl.DataFrame(rows), storage=pl.DataFrame(totals))


def assert_same(a, b):
    if len(a) != len(b):
        raise AssertionError("Rozna liczba wierszy wynikowych")
    for i, (ra, rb) in enumerate(zip(a, b)):
        if len(ra) != len(rb):
            raise AssertionError(f"Rozna liczba kolumn w wierszu {i}")
        for j, (va, vb) in enumerate(zip(ra, rb)):
            if va is None or vb is None:
                equal = va is None and vb is None
            elif isinstance(va, Real) and isinstance(vb, Real):
                if isinstance(va, Integral) and isinstance(vb, Integral):
                    equal = va == vb
                else:
                    equal = (math.isnan(va) and math.isnan(vb)) or math.isclose(
                        va, vb, rel_tol=1e-9, abs_tol=1e-8)
            else:
                equal = va == vb
            if not equal:
                raise AssertionError(f"Rozny wynik [{i},{j}]: {va!r} != {vb!r}")


def verify_data(ctx, report_name="validation.json"):
    """Zgodnosc wielozbiorow wg DuckDB, nie identycznosc bitowa plikow."""
    check_source(ctx)
    con = ctx["con"]
    readers = {"source": parquet_reader(ctx["source"]), **ctx["readers"]}
    schemas = {name: {r[0]: r[1] for r in con.execute(
        f"DESCRIBE SELECT * FROM {reader}").fetchall()} for name, reader in readers.items()}
    if not (schemas["source"] == schemas["A"] == schemas["B"]):
        raise AssertionError(f"Rozne schematy: {schemas}")
    if not REQUIRED.issubset(schemas["source"]):
        raise AssertionError("Brak wymaganych kolumn")
    columns = ", ".join(map(identifier, schemas["source"]))
    counts = {}
    for name, reader in readers.items():
        counts[name] = con.execute(f"SELECT count(*) FROM {reader}").fetchone()[0]
        if not counts[name]:
            raise AssertionError(f"Puste dane: {name}")
        invalid = con.execute(f"""SELECT count(*) FROM (
            SELECT kod_stacji, data_czas_utc FROM {reader}
            GROUP BY 1,2 HAVING count(*) > 1
                OR kod_stacji IS NULL OR data_czas_utc IS NULL
        ) AS invalid_keys""").fetchone()[0]
        if invalid:
            raise AssertionError(f"Nieprawidlowy klucz stacja-czas: {name}")
    for left, right in [("source", "A"), ("A", "B")]:
        for a, b in [(left, right), (right, left)]:
            different = con.execute(f"""SELECT count(*) FROM (
                SELECT {columns} FROM {readers[a]}
                EXCEPT ALL SELECT {columns} FROM {readers[b]}
            ) AS differences""").fetchone()[0]
            if different:
                raise AssertionError(f"Rozne dane {a} -> {b}: {different} wierszy")
    if len(set(counts.values())) != 1:
        raise AssertionError("Rozna liczebnosc")
    check_source(ctx)
    result = dict(status="OK", source_rows=counts["source"], A_rows=counts["A"],
                  B_rows=counts["B"], checked_columns=len(schemas["source"]),
                  equality="DuckDB EXCEPT ALL in both directions")
    if report_name is not None:
        save_json(ctx["report"] / report_name, result)
    return result


def query(reader, stations):
    if stations is None:
        predicate = ""
    else:
        if isinstance(stations, (str, bytes)):
            raise TypeError("stations musi byc kolekcja kodow")
        stations = list(stations)
        if not stations or any(not isinstance(s, str) or not s for s in stations):
            raise ValueError("Lista stacji musi byc niepusta i zawierac niepuste napisy")
        predicate = " WHERE kod_stacji IN (" + ", ".join(map(literal, stations)) + ")"
    return f"""SELECT godzina, avg(pm25) AS srednie_pm25,
        avg(temperature_2m) AS srednia_temperatura, avg(pm25_lag24) AS srednie_pm25_lag24,
        count(*) AS liczba_godzin, count(pm25) AS liczba_pomiarow
        FROM {reader}{predicate} GROUP BY godzina ORDER BY godzina"""


def prepare_scenarios(ctx):
    check_source(ctx)
    con = ctx["con"]
    stations = [r[0] for r in con.execute(
        f"SELECT DISTINCT kod_stacji FROM {ctx['readers']['A']} ORDER BY kod_stacji").fetchall()]
    if not stations or any(s is None for s in stations):
        raise ValueError("Brak stacji lub NULL w kodzie")
    random.Random(ctx["seed"]).shuffle(stations)
    sizes = sorted({min(n, len(stations)) for n in [1, 8, 32]})
    scenarios = [dict(id=f"q{i:02}", scenario=f"{n}_stacji", stations=stations[:n],
                      selected_stations=n, total_stations=len(stations))
                 for i, n in enumerate(sizes, 1)]
    scenarios.append(dict(id="control", scenario="wszystkie_stacje", stations=None,
                          selected_stations=len(stations), total_stations=len(stations)))
    readers = dict(ctx["readers"])
    if ctx.get("compare_glob", False):
        # C to ten sam zapis B, ale inny sposob odkrywania plikow.
        alternate = "glob" if ctx.get("reader_mode") == "all_files_list" else "all_files_list"
        if "partition_readers" not in ctx:
            raise RuntimeError("Brak readerow alternatywnych: wykonaj configure_readers(ctx)")
        readers["C"] = ctx["partition_readers"][alternate]
    ctx["variants"] = list(readers)
    ctx["scenarios"], ctx["queries"], ctx["expected"] = scenarios, {}, {}
    save_json(ctx["report"] / "benchmark_protocol.json", {
        "variants": ctx["variants"], "B_reader": ctx.get("reader_mode", "external"),
        "C_reader": alternate if "C" in readers else None,
        "repeats": ctx["repeats"], "warmups": ctx["warmups"],
        "comparison": "same SQL aggregation, only reader differs",
        "cache": "OS cache not cleared; validation and EXPLAIN ANALYZE precede timing",
        "file_list_contains": "all partitions, never only selected stations",
    })
    for item in scenarios:
        results = {}
        for variant, reader in readers.items():
            q = query(reader, item["stations"])
            ctx["queries"][item["id"], variant] = q
            results[variant] = con.execute(q).fetchall()
            (ctx["report"] / f"{item['id']}_{variant}.sql").write_text(q, encoding="utf-8")
            plan = con.execute("EXPLAIN ANALYZE " + q).fetchall()
            (ctx["report"] / f"plan_{item['id']}_{variant}.txt").write_text(
                "\n".join(str(r[-1]) for r in plan), encoding="utf-8")
        for variant in readers:
            assert_same(results["A"], results[variant])
        ctx["expected"][item["id"]] = results["A"]
        save_csv(ctx["report"] / f"aggregation_{item['id']}.csv",
                 [dict(zip(RESULT_COLUMNS, row)) for row in results["A"]])
    save_json(ctx["report"] / "scenarios.json", scenarios)
    save_csv(ctx["report"] / "scenarios.csv",
             [{**item, "stations": json.dumps(item["stations"], ensure_ascii=False)} for item in scenarios])
    return pl.DataFrame(scenarios)



@contextmanager
def publication_lock(path):
    try:
        handle = Path(path).open("x", encoding="utf-8")
    except FileExistsError as exc:
        raise RuntimeError(f"Publikacja zablokowana: {path}. Po awarii sprawdz PID przed usunieciem blokady.") from exc
    try:
        with handle:
            json.dump({"pid": os.getpid(), "created_utc": datetime.now(timezone.utc).isoformat()}, handle)
        yield
    finally:
        Path(path).unlink(missing_ok=True)


def publish_dataset(ctx):
    """Staging + backup + rollback. Nie jest to atomowa podmiana katalogu.

    Nie uruchamiaj publikacji podczas odczytu celu przez inne procesy.
    Blokada chroni tylko inne publikacje korzystajace z tej samej blokady.
    """
    silver = ctx["root"] / "data/silver"
    target = silver / "pm25_pogoda_2024_partycje"
    data_root = (ctx["root"] / "data").resolve()
    for path in [silver, ctx["data"], ctx["partitioned"], target]:
        if path.is_symlink() or not path.resolve().is_relative_to(data_root):
            raise ValueError(f"Niebezpieczna sciezka publikacji: {path}")
    if not ctx["partitioned"].is_dir():
        raise FileNotFoundError(ctx["partitioned"])
    with publication_lock(silver / ".pm25_pogoda_2024_partycje.publish.lock"):
        if target.exists() and not target.is_dir():
            raise ValueError(f"Cel nie jest katalogiem: {target}")
        previous = ctx["data"] / ("previous_published_" + uuid4().hex)
        with TemporaryDirectory(dir=silver, prefix=target.name + ".tmp-") as temp:
            staging = Path(temp) / "partitioned"
            for path in ctx["partitioned"].rglob("*"):
                if path.is_symlink() or not (path.is_file() or path.is_dir()):
                    raise ValueError(f"Niedozwolony plik: {path}")
            shutil.copytree(ctx["partitioned"], staging)
            validation = verify_data({**ctx, "readers": {
                "A": ctx["readers"]["A"], "B": parquet_reader(staging, True)}}, report_name=None)
            moved = False
            if target.exists():
                target.rename(previous)
                moved = True
            try:
                staging.rename(target)
            except BaseException:
                if moved and not target.exists():
                    try:
                        previous.rename(target)
                    except OSError as rollback_error:
                        raise RuntimeError(f"Rollback nieudany. Backup: {previous}") from rollback_error
                raise
        save_json(ctx["report"] / ("publication_" + uuid4().hex + ".json"), {
            "target": str(target), "previous": str(previous) if moved else None,
            "validation": validation, "published_utc": datetime.now(timezone.utc).isoformat()})
        (ctx["report"] / "published_path.txt").write_text(str(target), encoding="utf-8")
    return target


def run_benchmark(ctx):
    if not ctx.get("queries") or not ctx.get("expected"):
        raise RuntimeError("Najpierw wykonaj prepare_scenarios(ctx)")
    check_source(ctx)
    con = ctx["con"]
    variants = ctx.get("variants", ["A", "B"])
    rng = random.Random(ctx["seed"])
    keys = list(ctx["queries"])
    for _ in range(ctx["warmups"]):
        rng.shuffle(keys)
        for key in keys:
            assert_same(ctx["expected"][key[0]], con.execute(ctx["queries"][key]).fetchall())
    # Kontrola SHA-256 nie jest wykonywana pomiedzy mierzonymi zapytaniami.
    rows = []
    for iteration in range(1, ctx["repeats"] + 1):
        tasks = list(ctx["scenarios"])
        rng.shuffle(tasks)
        for item in tasks:
            order_variants = list(variants)
            rng.shuffle(order_variants)
            for position, variant in enumerate(order_variants, 1):
                start = perf_counter_ns()
                result = con.execute(ctx["queries"][item["id"], variant]).fetchall()
                elapsed = (perf_counter_ns() - start) / 1_000_000
                assert_same(ctx["expected"][item["id"]], result)
                rows.append(dict(id=item["id"], scenario=item["scenario"], iteration=iteration,
                                 variant=variant, position_in_pair=position,
                                 time_ms=elapsed, result_rows=len(result)))
    check_source(ctx)
    save_csv(ctx["report"] / "timings_raw.csv", rows)
    return pl.DataFrame(rows)


def summarize(ctx, timings):
    raw = timings.to_dicts()
    variants = ctx.get("variants", ["A", "B"])
    if not ctx.get("scenarios"):
        raise ValueError("Brak scenariuszy")
    expected = {(item["id"], v, i) for item in ctx["scenarios"] for v in variants
                for i in range(1, ctx["repeats"] + 1)}
    seen = set()
    for item in raw:
        key = item["id"], item["variant"], item["iteration"]
        if key not in expected or key in seen:
            raise ValueError(f"Nieoczekiwany lub zduplikowany pomiar: {key}")
        seen.add(key)
        if not math.isfinite(item["time_ms"]) or item["time_ms"] <= 0:
            raise ValueError(f"Nieprawidlowy czas: {item}")
    if seen != expected:
        raise ValueError(f"Brak pomiarow: {sorted(expected - seen)}")
    rows = []
    comparisons = []
    for scenario in ctx["scenarios"]:
        row = dict(id=scenario["id"], scenario=scenario["scenario"], repeats=ctx["repeats"])
        for variant in variants:
            values = [r["time_ms"] for r in raw if r["id"] == scenario["id"] and r["variant"] == variant]
            for name, value in [("mean", statistics.mean(values)), ("median", statistics.median(values)),
                                ("std", statistics.stdev(values)), ("min", min(values)), ("max", max(values))]:
                row[f"{variant}_{name}_ms"] = value
        a, b = row["A_mean_ms"], row["B_mean_ms"]
        row.update(time_saved_percent=100 * (a - b) / a, speedup_A_over_B=a / b)
        for variant in variants:
            if variant == "A":
                continue
            value = row[f"{variant}_mean_ms"]
            comparisons.append(dict(id=scenario["id"], scenario=scenario["scenario"],
                                    variant=variant, baseline="A", mean_ms=value,
                                    time_saved_percent=100 * (a - value) / a,
                                    speedup_A_over_variant=a / value))
        rows.append(row)
    save_csv(ctx["report"] / "summary.csv", rows)
    save_csv(ctx["report"] / "comparisons.csv", comparisons)
    columns = list(rows[0])
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join(["---"] * len(columns)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(f"{row[k]:.3f}" if isinstance(row[k], float)
                                       else str(row[k]) for k in columns) + " |")
    (ctx["report"] / "summary.md").write_text("\n".join(lines), encoding="utf-8")
    return pl.DataFrame(rows)


def make_plots(ctx, timings, summary):
    import matplotlib.pyplot as plt
    rows, raw = summary.to_dicts(), timings.to_dicts()
    if not rows:
        raise ValueError("Puste podsumowanie")
    variants = ctx.get("variants", ["A", "B"])
    labels = [r["scenario"] for r in rows]
    names = {"A": "A - plaski", "B": "B - partycje: " + ctx.get("reader_mode", "external"),
             "C": "C - partycje: reader alternatywny"}
    x, figures = list(range(len(rows))), []
    try:
        fig, ax = plt.subplots(figsize=(12, 6))
        figures.append(fig)
        width = .8 / len(variants)
        for j, variant in enumerate(variants):
            offset = (j - (len(variants) - 1) / 2) * width
            ax.bar([i + offset for i in x], [r[f"{variant}_mean_ms"] for r in rows],
                   width=width, yerr=[r[f"{variant}_std_ms"] for r in rows], capsize=4,
                   label=names.get(variant, variant))
        ax.set_xticks(x, labels, rotation=20, ha="right")
        ax.set_ylabel("Czas [ms], srednia +/- SD")
        ax.set_title("Benchmark - cache OS nieczyszczony")
        ax.legend()
        fig.tight_layout()
        fig.savefig(ctx["report"] / "01_mean_times.png", dpi=160)
        fig, ax = plt.subplots(figsize=(12, 6))
        figures.append(fig)
        alternatives = [v for v in variants if v != "A"]
        width = .8 / len(alternatives)
        for j, variant in enumerate(alternatives):
            values = [100 * (r["A_mean_ms"] - r[f"{variant}_mean_ms"]) / r["A_mean_ms"] for r in rows]
            offset = (j - (len(alternatives) - 1) / 2) * width
            positions = [i + offset for i in x]
            ax.bar(positions, values, width=width, label=names.get(variant, variant))
            for position, value in zip(positions, values):
                ax.annotate(f"{value:.1f}%", (position, value),
                            xytext=(0, 5 if value >= 0 else -5), textcoords="offset points",
                            ha="center", va="bottom" if value >= 0 else "top", fontsize=9)
        ax.axhline(0, color="black", linewidth=.8)
        ax.margins(y=.2)
        ax.set_xticks(x, labels, rotation=20, ha="right")
        ax.set_ylabel("Skrocenie czasu wzgledem A [%]")
        ax.legend()
        fig.tight_layout()
        fig.savefig(ctx["report"] / "02_time_saved.png", dpi=160)
        fig, axes = plt.subplots(len(rows), 1, figsize=(12, 3 * len(rows)), squeeze=False)
        figures.append(fig)
        for ax, item in zip(axes[:, 0], rows):
            for variant in variants:
                values = sorted((r for r in raw if r["id"] == item["id"] and r["variant"] == variant),
                                key=lambda r: r["iteration"])
                ax.plot([r["iteration"] for r in values], [r["time_ms"] for r in values],
                        marker=".", label=variant)
            ax.set_title(item["scenario"])
            ax.set_ylabel("Czas [ms]")
            ax.legend()
        axes[-1, 0].set_xlabel("Powtorzenie")
        fig.tight_layout()
        fig.savefig(ctx["report"] / "03_repetitions.png", dpi=160)
        return figures
    except BaseException:
        for figure in figures:
            plt.close(figure)
        raise


def main():
    # compare_glob=True dodaje C (te same partycje, ale glob).
    # Domyslnie dwa warianty, zgodnie z A/B w zadaniu.
    ctx = create_context(reader_mode="all_files_list", compare_glob=False)
    try:
        storage = write_variants(ctx)
        print(storage["tree"])
        print(verify_data(ctx))
        print(prepare_scenarios(ctx))
        timings = run_benchmark(ctx)
        summary = summarize(ctx, timings)
        print(summary)
        try:
            figures = make_plots(ctx, timings, summary)
        except ModuleNotFoundError as exc:
            if exc.name != "matplotlib":
                raise
            print("Brak matplotlib: CSV zapisane, wykresy pominiete")
        else:
            import matplotlib.pyplot as plt
            for figure in figures:
                plt.close(figure)
        print("Dane dla M3:", publish_dataset(ctx))
        print("Raport:", ctx["report"])
        return ctx["report"]
    finally:
        ctx["con"].close()


if __name__=='__main__':
    main()
