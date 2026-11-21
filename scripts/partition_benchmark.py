"""SILVER: trzy uklady danych i piec konfiguracji benchmarku.

A: plik plaski. B: niewiele zrownowazonych grup stacji. C: partycja na stacje.
Dla kazdego scenariusza: A, B, B_no_filter, C, C_no_filter.
No_filter usuwa caly WHERE i oblicza wynik dla calego zbioru.
Wskazniki przyspieszenia liczymy wylacznie dla zgodnego zakresu danych.
Caly zapis miesci sie w RAM. Snappy i wspolne ustawienia writera.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
from importlib.metadata import version, PackageNotFoundError
from numbers import Real, Integral
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter_ns
from uuid import uuid4
import csv
import hashlib
import json
import math
import os
import platform
import random
import re
import shutil
import statistics
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = {"kod_stacji", "data_czas_utc", "pm25", "godzina", "temperature_2m", "pm25_lag24"}
CONFIGS = ["A", "B", "B_no_filter", "C", "C_no_filter"]
LABELS = {"A": "A - plik plaski", "B": "B - grupy, filtr", "B_no_filter": "B - bez filtra",
          "C": "C - stacje, filtr", "C_no_filter": "C - bez filtra"}
RESULT_COLUMNS = ["godzina", "srednie_pm25", "srednia_temperatura", "srednie_pm25_lag24",
                  "liczba_godzin", "liczba_pomiarow"]


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


def parquet_reader(path, partition_key=None, files=None):
    path = Path(path)
    if partition_key is None:
        return f"read_parquet({literal(path.as_posix())}, hive_partitioning=false)"
    if partition_key not in {"station_group", "kod_stacji"}:
        raise ValueError("Nieznany klucz partycji")
    if files is None:
        argument = literal((path / f"{partition_key}=*" / "*.parquet").as_posix())
    else:
        files = list(files)
        if not files:
            raise ValueError("Pusta lista plikow")
        argument = "[" + ", ".join(literal(Path(p).as_posix()) for p in files) + "]"
    sql_type = "INTEGER" if partition_key == "station_group" else "VARCHAR"
    return (f"read_parquet({argument}, hive_partitioning=true, "
            f"hive_types={{{literal(partition_key)}: {literal(sql_type)}}})")


def create_context(root=ROOT, repeats=20, warmups=3, threads=None,
                   write_statistics=False, row_group_size=122880, seed=42,
                   group_count=8, reader_mode="all_files_list", small_difference_percent=5.0):
    repeats = checked_int(repeats, "repeats", 2)
    warmups = checked_int(warmups, "warmups", 0)
    threads = checked_int(min(4, os.cpu_count() or 1) if threads is None else threads, "threads", 1)
    row_group_size = checked_int(row_group_size, "row_group_size", 1)
    group_count = checked_int(group_count, "group_count", 1)
    seed = checked_int(seed, "seed", 0)
    if not isinstance(write_statistics, bool):
        raise TypeError("write_statistics musi byc bool")
    if reader_mode not in {"all_files_list", "glob"}:
        raise ValueError("reader_mode: all_files_list albo glob")
    if not isinstance(small_difference_percent, Real) or isinstance(small_difference_percent, bool):
        raise TypeError("small_difference_percent musi byc liczba")
    if not math.isfinite(small_difference_percent) or small_difference_percent < 0:
        raise ValueError("Nieprawidlowy prog malej roznicy")
    root = Path(root).resolve()
    source = root / "data/silver/pm25_pogoda_2024.parquet"
    if not source.is_file():
        raise FileNotFoundError(f"Najpierw utworz tabele cech w notebooku 05: {source}")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ") + "_" + uuid4().hex[:8]
    data, report = root / "data/benchmark" / stamp, root / "reports/partition_benchmark" / stamp
    con = duckdb.connect()
    try:
        con.execute(f"SET threads = {threads}")
        data.mkdir(parents=True, exist_ok=False)
        report.mkdir(parents=True, exist_ok=False)
        ctx = dict(root=root, source=source, flat=data / "A_flat.parquet",
                   grouped=data / "B_grouped", partitioned=data / "C_by_station",
                   data=data, report=report, repeats=repeats, warmups=warmups,
                   threads=threads, row_group_size=row_group_size, seed=seed,
                   group_count=group_count, reader_mode=reader_mode,
                   write_statistics=write_statistics, con=con,
                   small_difference_percent=float(small_difference_percent),
                   source_fingerprint=fingerprint(source))
        save_json(report / "environment.json", {
            "created_utc": stamp, "source": str(source), "source_fingerprint": ctx["source_fingerprint"],
            "python": platform.python_version(), "platform": platform.platform(),
            "logical_cpus": os.cpu_count(), "threads": threads, "repeats": repeats,
            "warmups": warmups, "seed": seed, "group_count_requested": group_count,
            "row_group_size": row_group_size, "compression": "snappy",
            "write_statistics": write_statistics, "reader_mode": reader_mode,
            "cache": "OS cache not cleared; validation warms cache",
            "timing": "execute + fetchall; validation and writing outside timer",
            "file_discovery": "all file lists built outside timer" if reader_mode == "all_files_list"
                              else "glob inside read_parquet",
            "writer": "pyarrow.parquet.write_table for A/B/C; full sorted table in RAM",
            "versions": {p: package_version(p) for p in ["duckdb", "pyarrow", "polars", "matplotlib"]},
        })
        return ctx
    except BaseException:
        con.close()
        raise


def balanced_station_groups(station_counts, group_count):
    """Deterministyczne zachlanne wyrownanie liczby wierszy, bez dzielenia stacji."""
    group_count = checked_int(group_count, "group_count", 1)
    pairs = [(str(station), checked_int(count, "station_rows", 1)) for station, count in station_counts]
    if not pairs or len({s for s, _ in pairs}) != len(pairs):
        raise ValueError("Brak stacji lub zduplikowana stacja")
    if len(pairs) > 1 and group_count >= len(pairs):
        raise ValueError("B musi miec mniej partycji niz C. Zmniejsz group_count.")
    k = min(group_count, len(pairs))
    loads = [0] * k
    mapping = {}
    for station, count in sorted(pairs, key=lambda x: (-x[1], x[0])):
        group = min(range(k), key=lambda g: (loads[g], g))
        mapping[station] = group
        loads[group] += count
    return mapping, loads


def configure_readers(ctx):
    use_list = ctx["reader_mode"] == "all_files_list"
    b_files = sorted(ctx["grouped"].glob("station_group=*/*.parquet"))
    c_files = sorted(ctx["partitioned"].glob("kod_stacji=*/*.parquet"))
    if not b_files or not c_files:
        raise FileNotFoundError("Brak zapisow B lub C")
    ctx["readers"] = {
        "A": parquet_reader(ctx["flat"]),
        "B": parquet_reader(ctx["grouped"], "station_group", b_files if use_list else None),
        "C": parquet_reader(ctx["partitioned"], "kod_stacji", c_files if use_list else None),
    }
    save_json(ctx["report"] / "reader_manifest.json", {
        "mode": ctx["reader_mode"],
        "B": [p.as_posix() for p in b_files], "C": [p.as_posix() for p in c_files],
        "lists_include": "all files, not just selected stations",
    })
    for key in ("queries", "scenarios", "expected", "variants"):
        ctx.pop(key, None)
    return ctx["readers"]


def logical_reader(ctx, variant):
    columns = ", ".join(map(identifier, ctx["columns"]))
    return f"(SELECT {columns} FROM {ctx['readers'][variant]})"


def write_variants(ctx):
    check_source(ctx)
    targets = [ctx["flat"], ctx["grouped"], ctx["partitioned"]]
    if any(p.exists() for p in targets):
        raise FileExistsError("Zapisy juz istnieja. Utworz nowy kontekst.")
    frame = pl.read_parquet(ctx["source"])
    if REQUIRED - set(frame.columns):
        raise ValueError(f"Brak kolumn: {sorted(REQUIRED - set(frame.columns))}")
    if "station_group" in frame.columns:
        raise ValueError("station_group to zarezerwowana nazwa techniczna")
    if frame.is_empty() or frame["kod_stacji"].null_count() or frame["data_czas_utc"].null_count():
        raise ValueError("Puste dane lub NULL w kluczu")
    if frame["kod_stacji"].dtype != pl.String:
        raise TypeError("kod_stacji musi miec typ String")
    if frame.select(pl.struct(["kod_stacji", "data_czas_utc"]).is_duplicated().any()).item():
        raise ValueError("Duplikaty klucza stacja-czas")
    counts = frame.group_by("kod_stacji").len().sort("kod_stacji")
    station_counts = list(counts.iter_rows())
    for station, _ in station_counts:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", station):
            raise ValueError(f"Kod stacji wymaga kodowania sciezki Hive: {station!r}")
    mapping, loads = balanced_station_groups(station_counts, ctx["group_count"])
    ctx["columns"], ctx["station_groups"] = frame.columns, mapping
    ctx["actual_group_count"] = len(loads)
    table = frame.sort(["kod_stacji", "data_czas_utc"]).to_arrow().combine_chunks()
    station_tables, offset = {}, 0
    for station, count in station_counts:
        station_tables[station] = table.slice(offset, count)
        offset += count
    options = dict(compression="snappy", write_statistics=ctx["write_statistics"],
                   row_group_size=ctx["row_group_size"])
    with TemporaryDirectory(dir=ctx["data"], prefix="write-") as temp:
        temp = Path(temp)
        a, b, c = temp / "A.parquet", temp / "B", temp / "C"
        b.mkdir(); c.mkdir()
        pq.write_table(table, a, **options)
        for group in range(len(loads)):
            folder = b / f"station_group={group}"
            folder.mkdir()
            tables = [station_tables[s] for s in sorted(mapping) if mapping[s] == group]
            pq.write_table(pa.concat_tables(tables), folder / "part-0.parquet", **options)
        for station in sorted(station_tables):
            folder = c / f"kod_stacji={station}"
            folder.mkdir()
            pq.write_table(station_tables[station].drop(["kod_stacji"]),
                           folder / "part-0.parquet", **options)
        check_source(ctx)
        moved = []
        try:
            for source, target in [(a, ctx["flat"]), (b, ctx["grouped"]), (c, ctx["partitioned"])]:
                source.rename(target)
                moved.append(target)
        except BaseException:
            for path in reversed(moved):
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink(missing_ok=True)
            raise
    save_csv(ctx["report"] / "station_groups.csv", [
        dict(kod_stacji=s, station_group=mapping[s], rows=n) for s, n in station_counts])
    save_json(ctx["report"] / "layout.json", {
        "columns": ctx["columns"], "station_groups": mapping, "group_rows": loads,
        "A": "flat", "B": "balanced station groups", "C": "one station per partition",
        "technical_column": "station_group is excluded from the logical schema",
    })
    configure_readers(ctx)
    return inspect_storage(ctx)


def inspect_storage(ctx):
    file_sets = {"A": [ctx["flat"]], "B": sorted(ctx["grouped"].glob("station_group=*/*.parquet")),
                 "C": sorted(ctx["partitioned"].glob("kod_stacji=*/*.parquet"))}
    totals, details, groups, tree = [], [], [], []
    for variant, paths in file_sets.items():
        if not paths:
            raise AssertionError(f"Brak plikow {variant}")
        nrows = nbytes = ngroups = 0
        tree.append(variant + ":")
        for path in paths:
            with pq.ParquetFile(path) as handle:
                meta = handle.metadata
                size = path.stat().st_size
                nrows += meta.num_rows; nbytes += size; ngroups += meta.num_row_groups
                details.append(dict(variant=variant, file=str(path.relative_to(ctx["data"])),
                                    rows=meta.num_rows, bytes=size, row_groups=meta.num_row_groups))
                tree.append(f"  {path.relative_to(ctx['data'])} ({meta.num_rows:,} wierszy, {size:,} B)")
                for g in range(meta.num_row_groups):
                    rg = meta.row_group(g)
                    groups.append(dict(variant=variant, file=str(path.relative_to(ctx["data"])),
                                       row_group=g, rows=rg.num_rows))
                    for c in range(rg.num_columns):
                        if rg.column(c).compression != "SNAPPY":
                            raise AssertionError(f"Kompresja inna niz Snappy: {path}")
        totals.append(dict(variant=variant, files=len(paths), rows=nrows, bytes=nbytes, row_groups=ngroups))
    if len({r["rows"] for r in totals}) != 1:
        raise AssertionError("Rozna liczba wierszy A/B/C")
    save_csv(ctx["report"] / "storage.csv", totals)
    save_csv(ctx["report"] / "partition_files.csv", details)
    save_csv(ctx["report"] / "row_groups.csv", groups)
    tree = "\n".join(tree)
    (ctx["report"] / "directory_tree.txt").write_text(tree, encoding="utf-8")
    return dict(tree=tree, storage=pl.DataFrame(totals), files=pl.DataFrame(details))


def verify_data(ctx, report_name="validation.json"):
    check_source(ctx)
    con = ctx["con"]
    readers = {"source": parquet_reader(ctx["source"]),
               **{v: logical_reader(ctx, v) for v in ["A", "B", "C"]}}
    schemas = {v: {r[0]: r[1] for r in con.execute(
        f"DESCRIBE SELECT * FROM {reader}").fetchall()} for v, reader in readers.items()}
    if any(schema != schemas["source"] for schema in schemas.values()):
        raise AssertionError(f"Niezgodne schematy logiczne: {schemas}")
    cols = ", ".join(map(identifier, ctx["columns"]))
    counts = {}
    for variant, reader in readers.items():
        counts[variant] = con.execute(f"SELECT count(*) FROM {reader}").fetchone()[0]
        invalid = con.execute(f"""SELECT count(*) FROM (
            SELECT kod_stacji, data_czas_utc FROM {reader}
            GROUP BY 1,2 HAVING count(*) > 1 OR kod_stacji IS NULL OR data_czas_utc IS NULL
        ) AS invalid_keys""").fetchone()[0]
        if not counts[variant] or invalid:
            raise AssertionError(f"Nieprawidlowe klucze lub puste dane: {variant}")
    for variant in ["A", "B", "C"]:
        for left, right in [("source", variant), (variant, "source")]:
            diff = con.execute(f"""SELECT count(*) FROM (
                SELECT {cols} FROM {readers[left]} EXCEPT ALL
                SELECT {cols} FROM {readers[right]}) AS differences""").fetchone()[0]
            if diff:
                raise AssertionError(f"Niezgodne wartosci {left} -> {right}: {diff}")
    if len(set(counts.values())) != 1:
        raise AssertionError("Rozna liczba wierszy")
    cases = " ".join(f"WHEN {literal(s)} THEN {g}" for s, g in sorted(ctx["station_groups"].items()))
    invalid = con.execute(f"""SELECT count(*) FROM {ctx['readers']['B']}
        WHERE station_group IS DISTINCT FROM (CASE kod_stacji {cases} ELSE NULL END)
    """).fetchone()[0]
    if invalid:
        raise AssertionError("B: niezgodne przypisanie stacji do grupy")
    check_source(ctx)
    result = dict(status="OK", source_rows=counts["source"], A_rows=counts["A"],
                  B_rows=counts["B"], C_rows=counts["C"], checked_columns=len(ctx["columns"]))
    if report_name is not None:
        save_json(ctx["report"] / report_name, result)
    return result


def query(reader, stations=None, station_groups=None):
    predicates = []
    if stations is not None:
        if isinstance(stations, (str, bytes)):
            raise TypeError("stations musi byc lista kodow")
        stations = list(stations)
        if not stations or any(not isinstance(s, str) or not s for s in stations):
            raise ValueError("Nieprawidlowa lista stacji")
        predicates.append("kod_stacji IN (" + ", ".join(map(literal, stations)) + ")")
    if station_groups is not None:
        if stations is None:
            raise ValueError("Filtr grup stosujemy tylko z filtrem stacji")
        groups = sorted({checked_int(g, "station_group", 0) for g in station_groups})
        if not groups:
            raise ValueError("Pusta lista grup")
        predicates.insert(0, "station_group IN (" + ", ".join(map(str, groups)) + ")")
    where = " WHERE " + " AND ".join(predicates) if predicates else ""
    return f"""SELECT godzina, avg(pm25) AS srednie_pm25,
        avg(temperature_2m) AS srednia_temperatura, avg(pm25_lag24) AS srednie_pm25_lag24,
        count(*) AS liczba_godzin, count(pm25) AS liczba_pomiarow
        FROM {reader}{where} GROUP BY godzina ORDER BY godzina"""


def prepare_scenarios(ctx):
    check_source(ctx)
    stations = sorted(ctx["station_groups"])
    random.Random(ctx["seed"]).shuffle(stations)
    # Kazdy wybor jest rzeczywistym podzbiorem. Pelny zbior jest osobna kontrola.
    sizes = sorted({n for n in [1, 8, 32] if n < len(stations)})
    if not sizes and len(stations) > 1:
        sizes = [1]
    scenarios = [dict(id=f"q{i:02}", scenario=f"{n}_stacji", stations=stations[:n],
                      selected_stations=n, total_stations=len(stations))
                 for i, n in enumerate(sizes, 1)]
    scenarios.append(dict(id="control", scenario="wszystkie_stacje", stations=None,
                          selected_stations=len(stations), total_stations=len(stations)))
    ctx["scenarios"], ctx["variants"] = scenarios, list(CONFIGS)
    ctx["queries"], ctx["expected"] = {}, {}
    full_a = ctx["con"].execute(query(ctx["readers"]["A"])).fetchall()
    for item in scenarios:
        selected = item["stations"]
        selected_groups = None if selected is None else sorted({ctx["station_groups"][s] for s in selected})
        item["selected_groups"] = len(set(ctx["station_groups"].values())) if selected_groups is None else len(selected_groups)
        queries = {
            "A": query(ctx["readers"]["A"], selected),
            "B": query(ctx["readers"]["B"], selected, selected_groups),
            "B_no_filter": query(ctx["readers"]["B"]),
            "C": query(ctx["readers"]["C"], selected),
            "C_no_filter": query(ctx["readers"]["C"]),
        }
        filtered_a = ctx["con"].execute(queries["A"]).fetchall()
        for config, sql in queries.items():
            scope = "all" if selected is None or config.endswith("no_filter") else "selected"
            expected = full_a if scope == "all" else filtered_a
            actual = ctx["con"].execute(sql).fetchall()
            assert_same(expected, actual)
            ctx["queries"][item["id"], config] = sql
            ctx["expected"][item["id"], config] = expected
            (ctx["report"] / f"{item['id']}_{config}.sql").write_text(sql, encoding="utf-8")
            plan = ctx["con"].execute("EXPLAIN ANALYZE " + sql).fetchall()
            (ctx["report"] / f"plan_{item['id']}_{config}.txt").write_text(
                "\n".join(str(r[-1]) for r in plan), encoding="utf-8")
        save_csv(ctx["report"] / f"aggregation_{item['id']}_selected.csv",
                 [dict(zip(RESULT_COLUMNS, row)) for row in filtered_a])
    save_csv(ctx["report"] / "aggregation_all.csv", [dict(zip(RESULT_COLUMNS, r)) for r in full_a])
    save_json(ctx["report"] / "scenarios.json", scenarios)
    save_json(ctx["report"] / "protocol.json", {
        "configs": CONFIGS, "no_filter": "no WHERE; aggregate all stations",
        "B_filter": "station_group IN (...) AND kod_stacji IN (...)",
        "C_filter": "kod_stacji IN (...)",
        "full_reference": "A no-filter validated outside timer; timed A full in control scenario",
        "comparability": "Never compare a filtered result to an unfiltered result as equal work",
        "repeated_full_controls": "B/C no-filter rerun in every scenario, not new independent datasets",
    })
    return pl.DataFrame(scenarios)


def run_benchmark(ctx):
    if not ctx.get("queries"):
        raise RuntimeError("Najpierw prepare_scenarios(ctx)")
    check_source(ctx)
    rng = random.Random(ctx["seed"])
    keys = list(ctx["queries"])
    for _ in range(ctx["warmups"]):
        rng.shuffle(keys)
        for key in keys:
            assert_same(ctx["expected"][key], ctx["con"].execute(ctx["queries"][key]).fetchall())
    rows = []
    for iteration in range(1, ctx["repeats"] + 1):
        scenarios = list(ctx["scenarios"])
        rng.shuffle(scenarios)
        for item in scenarios:
            configs = list(CONFIGS)
            rng.shuffle(configs)
            for position, config in enumerate(configs, 1):
                key = item["id"], config
                start = perf_counter_ns()
                actual = ctx["con"].execute(ctx["queries"][key]).fetchall()
                elapsed = (perf_counter_ns() - start) / 1_000_000
                assert_same(ctx["expected"][key], actual)
                scope = "all" if item["stations"] is None or config.endswith("no_filter") else "selected"
                rows.append(dict(id=item["id"], scenario=item["scenario"], iteration=iteration,
                                 variant=config, scope=scope, position_in_round=position,
                                 time_ms=elapsed, result_rows=len(actual)))
    check_source(ctx)
    save_csv(ctx["report"] / "timings_raw.csv", rows)
    return pl.DataFrame(rows)


def summarize(ctx, timings):
    raw = timings.to_dicts()
    expected_keys = {(s["id"], v, i) for s in ctx["scenarios"] for v in CONFIGS
                     for i in range(1, ctx["repeats"] + 1)}
    seen = set()
    scenarios = {s["id"]: s for s in ctx["scenarios"]}
    for r in raw:
        key = r["id"], r["variant"], r["iteration"]
        if key not in expected_keys or key in seen:
            raise ValueError(f"Nieprawidlowy lub zduplikowany pomiar: {key}")
        scope = "all" if scenarios[r["id"]]["stations"] is None or r["variant"].endswith("no_filter") else "selected"
        if r["scope"] != scope or not math.isfinite(r["time_ms"]) or r["time_ms"] <= 0:
            raise ValueError(f"Nieprawidlowy zakres/czas: {r}")
        seen.add(key)
    if seen != expected_keys:
        raise ValueError("Brakuje pomiarow")
    rows, indexed = [], {}
    for item in ctx["scenarios"]:
        for variant in CONFIGS:
            data = [r for r in raw if r["id"] == item["id"] and r["variant"] == variant]
            times = [r["time_ms"] for r in data]
            row = dict(id=item["id"], scenario=item["scenario"], variant=variant, scope=data[0]["scope"],
                       repeats=len(times), mean_ms=statistics.mean(times), median_ms=statistics.median(times),
                       std_ms=statistics.stdev(times), min_ms=min(times), max_ms=max(times))
            rows.append(row); indexed[item["id"], variant] = row
    comparisons = []
    for item in ctx["scenarios"]:
        pairs = [("A", "B"), ("A", "C"), ("C", "B"), ("C_no_filter", "B_no_filter")]
        if item["stations"] is None:
            pairs += [("A", "B_no_filter"), ("A", "C_no_filter")]
        for baseline, target in pairs:
            a, b = indexed[item["id"], baseline], indexed[item["id"], target]
            if a["scope"] != b["scope"]:
                raise AssertionError("Proba porownania roznych zakresow")
            base_times = {r["iteration"]: r["time_ms"] for r in raw if r["id"] == item["id"] and r["variant"] == baseline}
            target_times = {r["iteration"]: r["time_ms"] for r in raw if r["id"] == item["id"] and r["variant"] == target}
            diffs = [base_times[i] - target_times[i] for i in sorted(base_times)]
            delta = a["mean_ms"] - b["mean_ms"]
            gain = 100 * delta / a["mean_ms"]
            sd = statistics.stdev(diffs)
            small = abs(gain) <= ctx["small_difference_percent"] or abs(delta) <= sd
            outcome = "small_or_variable" if small else ("improvement" if gain > 0 else "regression")
            comparisons.append(dict(id=item["id"], scenario=item["scenario"], scope=a["scope"],
                                    baseline=baseline, target=target, baseline_ms=a["mean_ms"],
                                    target_ms=b["mean_ms"], time_saved_percent=gain,
                                    speedup=a["mean_ms"] / b["mean_ms"], paired_sd_ms=sd, outcome=outcome))
    save_csv(ctx["report"] / "summary.csv", rows)
    save_csv(ctx["report"] / "comparisons.csv", comparisons)
    ctx["comparison_rows"] = comparisons
    columns = list(rows[0])
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join(["---"] * len(columns)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(f"{row[k]:.3f}" if isinstance(row[k], float) else str(row[k]) for k in columns) + " |")
    (ctx["report"] / "summary.md").write_text("\n".join(lines), encoding="utf-8")
    return pl.DataFrame(rows)



def make_plots(ctx, timings, summary):
    import matplotlib.pyplot as plt
    rows, raw = summary.to_dicts(), timings.to_dicts()
    if not rows:
        raise ValueError("Brak podsumowania")
    scenarios = ctx["scenarios"]
    indexed = {(r["id"], r["variant"]): r for r in rows}
    colors = {"A": "#4C78A8", "B": "#2CA58D", "B_no_filter": "#8AD9C5",
              "C": "#E39C37", "C_no_filter": "#F4D08A"}
    figures = []
    try:
        fig, ax = plt.subplots(figsize=(14, 6))
        figures.append(fig)
        width = .15
        for j, variant in enumerate(CONFIGS):
            ax.bar([i + (j - 2) * width for i in range(len(scenarios))],
                   [indexed[s["id"], variant]["mean_ms"] for s in scenarios], width=width,
                   yerr=[indexed[s["id"], variant]["std_ms"] for s in scenarios],
                   capsize=3, label=LABELS[variant], color=colors[variant])
        ax.set_xticks(range(len(scenarios)), [s["scenario"] for s in scenarios])
        ax.set_ylabel("Czas [ms], srednia +/- SD")
        ax.set_title("Piec konfiguracji; bez filtra = inny zakres danych niz zapytanie filtrowane")
        ax.legend(fontsize=9)
        fig.tight_layout(); fig.savefig(ctx["report"] / "01_mean_times.png", dpi=160)

        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        figures.append(fig)
        for variant, offset in [("B", -.18), ("C", .18)]:
            values = [100 * (indexed[s["id"], "A"]["mean_ms"] - indexed[s["id"], variant]["mean_ms"])
                      / indexed[s["id"], "A"]["mean_ms"] for s in scenarios]
            axes[0].bar([i + offset for i in range(len(scenarios))], values, .36,
                        color=colors[variant], label=variant + " wzgledem A")
        axes[0].set_xticks(range(len(scenarios)), [s["scenario"] for s in scenarios], rotation=20)
        axes[0].set_title("Zgodny zakres: A/B/C")
        axes[0].set_ylabel("Zysk czasu [%]"); axes[0].axhline(0, color="black", lw=.7); axes[0].legend()
        full_gains = [100 * (indexed[s["id"], "C_no_filter"]["mean_ms"] - indexed[s["id"], "B_no_filter"]["mean_ms"])
                      / indexed[s["id"], "C_no_filter"]["mean_ms"] for s in scenarios]
        axes[1].bar(range(len(scenarios)), full_gains, color=colors["B"])
        axes[1].set_xticks(range(len(scenarios)), [s["scenario"] for s in scenarios], rotation=20)
        axes[1].set_title("Pelny zbior: B bez filtra wzgledem C bez filtra")
        axes[1].set_ylabel("Zysk czasu [%]"); axes[1].axhline(0, color="black", lw=.7)
        fig.tight_layout(); fig.savefig(ctx["report"] / "02_time_saved.png", dpi=160)

        fig, axes = plt.subplots(len(scenarios), 1, figsize=(14, 3.1 * len(scenarios)), squeeze=False)
        figures.append(fig)
        for ax, scenario in zip(axes[:, 0], scenarios):
            for variant in CONFIGS:
                values = sorted((r for r in raw if r["id"] == scenario["id"] and r["variant"] == variant),
                                key=lambda r: r["iteration"])
                ax.plot([r["iteration"] for r in values], [r["time_ms"] for r in values],
                        marker=".", label=LABELS[variant], color=colors[variant])
            ax.set_title(scenario["scenario"]); ax.set_ylabel("Czas [ms]")
            ax.legend(fontsize=8, ncol=3)
        axes[-1, 0].set_xlabel("Powtorzenie")
        fig.tight_layout(); fig.savefig(ctx["report"] / "03_repetitions.png", dpi=160)
        return figures
    except BaseException:
        for fig in figures:
            plt.close(fig)
        raise


def describe_results(ctx, summary, storage):
    if "comparison_rows" not in ctx:
        raise RuntimeError("Najpierw wykonaj summarize(ctx, timings)")
    sizes = {r["variant"]: r for r in storage["storage"].to_dicts()}
    lines = ["# Interpretacja wynikow", "## Uklady danych"]
    for variant in ["A", "B", "C"]:
        r = sizes[variant]
        lines.append(f"{variant}: {r['rows']:,} wierszy, {r['files']} plikow, "
                     f"{r['row_groups']} grup wierszy, {r['bytes'] / 1_000_000:.2f} MB.")
    lines += [
        ("B grupuje cale stacje w niewielu partycjach, wyrownujac liczbe wierszy. "
         "Nie jest to gwarancja najlepszego podzialu: mniej plikow oznacza mniejszy narzut, "
         "ale dla jednej stacji trzeba przeczytac takze dane innych stacji z jej grupy."),
        ("Wyniki A, B i C w scenariuszu filtrowanym dotycza tych samych stacji. "
         "B bez filtra i C bez filtra agreguja caly zbior; nie sa porownywane z filtrowanym A "
         "jako ten sam zakres pracy. Pelny A jest mierzony w scenariuszu wszystkie_stacje."),
        (f"Przyjeto prog niewielkiej zmiany {ctx['small_difference_percent']:g}% "
         "lub roznice srednich nieprzekraczajaca SD sparowanych roznic. "
         "To regula opisowa, nie test istotnosci. Cache OS nie byl czyszczony."),
    ]
    totals = {"B": {"improvement": 0, "regression": 0, "small_or_variable": 0},
              "C": {"improvement": 0, "regression": 0, "small_or_variable": 0}}
    for scenario in ctx["scenarios"]:
        lines.append("## " + scenario["scenario"])
        if scenario["stations"] is not None:
            lines.append(f"Wybrano {scenario['selected_stations']} stacji; "
                         f"filtr B obejmuje {scenario['selected_groups']} z {ctx['actual_group_count']} grup.")
        for r in [r for r in ctx["comparison_rows"] if r["id"] == scenario["id"]]:
            title = f"{LABELS[r['target']]} wzgledem {LABELS[r['baseline']]}"
            lines.append("### " + title)
            lines.append(f"Sredni czas: {r['target_ms']:.3f} ms vs {r['baseline_ms']:.3f} ms. "
                         f"Zysk czasu: {r['time_saved_percent']:+.2f}%. "
                         f"SD sparowanych roznic: {r['paired_sd_ms']:.3f} ms.")
            if r["baseline"] == "A" and r["target"] in totals:
                totals[r["target"]][r["outcome"]] += 1
            if r["outcome"] == "small_or_variable":
                reason = ("Roznica jest niewielka lub porownywalna z rozrzutem pomiarow. "
                          "Nie ma podstaw, by na tej serii uznac przewage za wyrazna i stabilna. "
                          "Warto powtorzyc test w stalych warunkach.")
            elif r["scope"] == "all":
                if r["time_saved_percent"] > 0:
                    reason = ("Stwierdzono krotszy czas pelnej agregacji. Poniewaz wynik obejmuje wszystkie stacje, "
                              "nie nalezy przypisywac poprawy pomijaniu stacji. Mozliwe znaczenie ma liczba plikow, "
                              "uklad grup wierszy i rownoleglosc; ich wplyw wymaga profilowania.")
                else:
                    reason = ("Stwierdzono dluzszy czas pelnej agregacji. Bez ograniczenia do wybranych stacji "
                              "nie wystepuje korzysc z pominiecia niepotrzebnych stacji. Narzut plikow i metadanych "
                              "moze przewyzszac korzysci fizycznego ukladu danych.")
            elif r["baseline"] == "C" and r["target"] == "B":
                if r["time_saved_percent"] > 0:
                    reason = ("Mniejsza liczba partycji skrocila czas tego zapytania. Wynik jest zgodny z hipoteza, "
                              "ze redukcja narzutu malych plikow zrekompensowala slabsza selektywnosc grup. "
                              "Nie dowodzi to, ze wybrana liczba grup jest globalnie optymalna.")
                else:
                    reason = ("Grupowanie stacji wydluzylo czas wzgledem podzialu na stacje. "
                              "Mozliwym powodem jest odczyt danych dodatkowych stacji w wybranych grupach; "
                              "oszczednosc liczby plikow nie wystarczyla w tym pomiarze.")
            elif r["time_saved_percent"] > 0:
                reason = ("Partycjonowanie skrocilo czas wzgledem pliku plaskiego dla tych samych stacji. "
                          "Wynik jest zgodny z korzyscia z ograniczenia odczytu, jezeli plan potwierdza pruning. "
                          "Nie nalezy przenosic tego wniosku na agregacje bez filtra.")
            else:
                reason = ("Partycjonowanie wydluzylo czas wzgledem pliku plaskiego. Zbior moze byc zbyt maly, "
                          "aby oszczednosc odczytu zrekompensowala narzut plikow, metadanych i planowania. "
                          "Przy rozgrzanym cache rola fizycznego transferu moze byc mniejsza. "
                          "To mozliwe wyjasnienia, a nie przyczyny udowodnione samym czasem.")
            lines.append(reason)
    lines.append("## Podsumowanie")
    for variant, counts in totals.items():
        lines.append(f"{variant} wzgledem A: poprawa w {counts['improvement']} scenariuszach, "
                     f"pogorszenie w {counts['regression']}, "
                     f"niewielka lub niestabilna roznica w {counts['small_or_variable']}.")
    for variant in ["B", "C"]:
        change = 100 * (sizes[variant]["bytes"] - sizes["A"]["bytes"]) / sizes["A"]["bytes"]
        lines.append(f"Zmiana rozmiaru {variant} wzgledem A: {change:+.2f}%.")
    lines.append("Kontrole bez filtra powtarzane przy roznych scenariuszach dotycza tego samego pelnego zbioru. "
                 "Nie sa dodatkowymi niezaleznymi zbiorami testowymi. Nie wyliczamy kosztow chmury ani "
                 "fizycznie odczytanych bajtow na podstawie samego czasu. Plany SQL sluza do oceny pruning.")
    text = "\n\n".join(lines)
    (ctx["report"] / "interpretation.md").write_text(text, encoding="utf-8")
    return text


def main():
    ctx = create_context(group_count=8)
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
            print("Brak matplotlib: pomiary i CSV zapisane, wykresy pominiete")
        else:
            import matplotlib.pyplot as plt
            for figure in figures:
                plt.close(figure)
        print(describe_results(ctx, summary, storage))
        print("Raport:", ctx["report"])
        return ctx["report"]
    finally:
        ctx["con"].close()


if __name__ == "__main__":
    main()

