"""A/B/C benchmark: K and 3K station groups, cached preparation, optional full validation.
A/B/C share the PyArrow writer, compression, statistics and row-group settings.
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
CONFIGS = ["A", "B", "C"]
LABELS = {"A": "A - plik plaski", "B": "B - K grup", "C": "C - 3K grup"}
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
    if file_state(ctx["original_source"]) != ctx["original_state"]:
        raise RuntimeError("Oryginalne zrodlo zmienilo sie podczas eksperymentu")
    if file_state(ctx["source"]) != ctx["source_state"]:
        raise RuntimeError("Zrodlo eksperymentu zmienilo sie podczas eksperymentu")

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



def build_scaled_source(con, source, data_dir, factor, row_group_size):
    factor = checked_int(factor, "scale_factor", 1)
    with pq.ParquetFile(source) as handle:
        n = handle.metadata.num_rows
    if not n:
        raise ValueError("Puste zrodlo")
    reader = parquet_reader(source)
    if factor == 1:
        return dict(scale_factor=1, original_source=str(source), source=str(source),
                    original_rows=n, expected_rows=n, actual_rows=n, synthetic=False)
    target = Path(data_dir) / f"source_x{factor}.parquet"
    if target.exists():
        raise FileExistsError(target)
    # Exactly N copies, with unique station suffixes; no timestamp changes.
    copies = [f"SELECT * REPLACE (kod_stacji || '__copy_{i:03d}' AS kod_stacji) FROM {reader}"
              for i in range(1, factor + 1)]
    temporary = target.with_suffix(".partial")
    try:
        con.execute(f"COPY ({' UNION ALL '.join(copies)}) TO {literal(temporary.as_posix())} "
                    f"(FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE {row_group_size})")
        with pq.ParquetFile(temporary) as handle:
            actual = handle.metadata.num_rows
        if actual != factor * n:
            raise AssertionError("Skalowanie nie zachowalo liczby rekordow")
        temporary.rename(target)
    finally:
        temporary.unlink(missing_ok=True)
    return dict(scale_factor=factor, original_source=str(source), source=str(target),
                original_rows=n, expected_rows=n*factor, actual_rows=actual,
                synthetic=True, method="N copies; station suffix __copy_NNN; all other columns unchanged")



PREPARATION_VERSION = "groups_3x_copy_ids_fast_v1"


def file_state(path):
    path = Path(path).resolve()
    stat = path.stat()
    return dict(path=str(path), bytes=stat.st_size, mtime_ns=stat.st_mtime_ns)


def cached_fingerprint(source, cache_root):
    state = file_state(source)
    key = hashlib.sha256(str(Path(source).resolve()).encode()).hexdigest()
    index = Path(cache_root) / f"source_{key}.json"
    if index.is_file():
        try:
            saved = json.loads(index.read_text(encoding="utf-8"))
            if saved["state"] == state:
                return saved["fingerprint"]
        except (ValueError, KeyError, OSError):
            pass
    print("SHA-256 zrodla: pierwszy odczyt lub zmieniony plik", flush=True)
    result = fingerprint(source)
    if file_state(source) != state:
        raise RuntimeError("Zrodlo zmienilo sie podczas identyfikacji")
    save_json(index, dict(state=state, fingerprint=result))
    return result


def cache_files(data):
    return {p.relative_to(data).as_posix(): file_state(p)
            for p in sorted(Path(data).rglob("*.parquet"))}


def release_lock(ctx):
    if ctx.get("lock_owned"):
        ctx["lock"].unlink(missing_ok=True)
        ctx["lock_owned"] = False


def create_context(root=ROOT, repeats=20, warmups=3, threads=None,
                   write_statistics=False, row_group_size=122880, seed=42,
                   group_count=8, reader_mode="all_files_list", small_difference_percent=5.0,
                   source_path=None, scale_factor=1, scenario_sizes=(1,8,32),
                   memory_limit=None, run_label="benchmark", validation_mode="quick",
                   reuse_prepared_data=True, generate_plots=True):
    root = Path(root).resolve()
    repeats = checked_int(repeats, "repeats", 2)
    warmups = checked_int(warmups, "warmups", 0)
    threads = checked_int(min(4, os.cpu_count() or 1) if threads is None else threads, "threads", 1)
    group_count = checked_int(group_count, "group_count", 1)
    scale_factor = checked_int(scale_factor, "scale_factor", 1)
    row_group_size = checked_int(row_group_size, "row_group_size", 1)
    seed = checked_int(seed, "seed", 0)
    scenario_sizes = sorted({checked_int(n, "scenario_size", 1) for n in scenario_sizes})
    if validation_mode not in {"quick", "full"}:
        raise ValueError("validation_mode: quick lub full")
    if reader_mode not in {"all_files_list", "glob"}:
        raise ValueError("reader_mode: all_files_list lub glob")
    for value in (write_statistics, reuse_prepared_data, generate_plots):
        if not isinstance(value, bool):
            raise TypeError("Opcje logiczne musza byc bool")
    if not isinstance(small_difference_percent, Real) or isinstance(small_difference_percent, bool) or not math.isfinite(small_difference_percent) or small_difference_percent < 0:
        raise ValueError("Niepoprawny small_difference_percent")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_label):
        raise ValueError("run_label: tylko litery ASCII, cyfry, _ i -")
    source = root / "data/silver/pm25_pogoda_2024.parquet" if source_path is None else Path(source_path)
    source = (source if source.is_absolute() else root/source).resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    original_state = file_state(source)
    cache_root = root / "data/benchmark_cache"
    cache_root.mkdir(parents=True, exist_ok=True)
    original_fingerprint = cached_fingerprint(source, cache_root)
    recipe = dict(version=PREPARATION_VERSION, source_sha256=original_fingerprint["sha256"],
                  scale_factor=scale_factor, group_count=group_count, c_group_count=3*group_count,
                  row_group_size=row_group_size, write_statistics=write_statistics,
                  pyarrow=package_version("pyarrow"), polars=package_version("polars"),
                  duckdb=package_version("duckdb"))
    key = hashlib.sha256(json.dumps(recipe,sort_keys=True).encode()).hexdigest()[:24]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ") + "_" + uuid4().hex[:8]
    data = cache_root/key if reuse_prepared_data else root/"data/benchmark"/f"x{scale_factor}_{stamp}"
    report = root/"reports/partition_benchmark"/f"{run_label}_x{scale_factor}_{stamp}"
    lock = data.with_name(data.name + ".lock")
    lock.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(lock), os.O_CREAT|os.O_EXCL|os.O_WRONLY)
    except FileExistsError:
        raise RuntimeError(f"Przygotowane dane sa zajete lub zostala blokada po awarii: {lock}. "
                           "Usun ja tylko jesli zaden przebieg nie pracuje.")
    os.close(fd)
    con = None
    try:
        cached = None
        manifest_path = data/"prepared.json"
        if reuse_prepared_data and manifest_path.is_file():
            try:
                candidate = json.loads(manifest_path.read_text(encoding="utf-8"))
                if candidate["recipe"] == recipe and candidate["files"] == cache_files(data):
                    cached = candidate
            except (OSError, KeyError, ValueError):
                pass
        if cached is None and data.exists():
            # This directory is exclusively owned by this preparation key.
            shutil.rmtree(data)
        data.mkdir(parents=True, exist_ok=True)
        report.mkdir(parents=True, exist_ok=False)
        con = duckdb.connect()
        con.execute(f"SET threads={threads}")
        if memory_limit is not None:
            con.execute("SET memory_limit=" + literal(memory_limit))
        if cached is None:
            print(f"Przygotowanie zrodla {scale_factor}x", flush=True)
            scaling = build_scaled_source(con, source, data, scale_factor, row_group_size)
        else:
            print(f"Cache {scale_factor}x: pomijam generowanie i zapis A/B/C", flush=True)
            scaling = dict(cached["scaling"])
            scaling["original_source"] = str(source)
            if scale_factor == 1:
                scaling["source"] = str(source)
        experiment_source = Path(scaling["source"])
        ctx = dict(root=root, source=experiment_source, original_source=source,
            original_state=original_state, source_state=file_state(experiment_source),
            flat=data/"A_flat.parquet", grouped=data/"B_grouped", partitioned=data/"C_grouped",
            data=data, report=report, repeats=repeats, warmups=warmups, threads=threads,
            row_group_size=row_group_size, seed=seed, group_count=group_count,
            c_group_count=3*group_count, reader_mode=reader_mode, write_statistics=write_statistics,
            con=con, small_difference_percent=float(small_difference_percent),
            scale_factor=scale_factor, scenario_sizes=scenario_sizes, scaling=scaling,
            validation_mode=validation_mode, cache_recipe=recipe, cached_manifest=cached,
            reused=cached is not None, lock=lock, lock_owned=True, generate_plots=generate_plots)
        check_source(ctx)
        save_json(report/"scaling.json", scaling)
        save_json(report/"environment.json", dict(created_utc=stamp, source=str(source),
            source_fingerprint=original_fingerprint, preparation=recipe, reused=ctx["reused"],
            python=platform.python_version(),platform=platform.platform(),threads=threads,
            repeats=repeats,warmups=warmups,seed=seed,scenario_sizes=scenario_sizes,
            reader_mode=reader_mode,validation_mode=validation_mode,memory_limit=memory_limit,
            timing="execute + fetchall; result comparison outside timer",
            cache="OS cache not cleared; validation and warmups warm it",
            file_identity="SHA-256 cached by source path/size/mtime; later stat checks, not cryptographic revalidation",
            writer="PyArrow write_table; Snappy; same statistics/row group settings for A/B/C"))
        return ctx
    except BaseException:
        if con is not None:
            con.close()
        lock.unlink(missing_ok=True)
        raise


def balanced_station_groups(station_counts, group_count):
    """Deterministyczne zachlanne wyrownanie liczby wierszy, bez dzielenia stacji."""
    group_count = checked_int(group_count, "group_count", 1)
    pairs = [(str(station), checked_int(count, "station_rows", 1)) for station, count in station_counts]
    if not pairs or len({s for s, _ in pairs}) != len(pairs):
        raise ValueError("Brak stacji lub zduplikowana stacja")
    if group_count > len(pairs):
        raise ValueError("Za malo stacji na C=3*B. Zmniejsz GROUP_COUNT.")
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
    c_files = sorted(ctx["partitioned"].glob("station_group=*/*.parquet"))
    if not b_files or not c_files:
        raise FileNotFoundError("Brak zapisow B lub C")
    ctx["readers"] = {
        "A": parquet_reader(ctx["flat"]),
        "B": parquet_reader(ctx["grouped"], "station_group", b_files if use_list else None),
        "C": parquet_reader(ctx["partitioned"], "station_group", c_files if use_list else None),
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
    if ctx["reused"]:
        saved = ctx["cached_manifest"]
        for key in ("columns", "station_groups", "c_station_groups", "actual_group_count", "actual_c_group_count"):
            ctx[key] = saved[key]
        static = ctx["data"]/"static_reports"
        if static.is_dir():
            for path in static.iterdir():
                if path.is_file():
                    shutil.copy2(path,ctx["report"]/path.name)
        configure_readers(ctx)
        return inspect_storage(ctx)
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
    if ctx["validation_mode"] == "full" and frame.select(pl.struct(["kod_stacji", "data_czas_utc"]).is_duplicated().any()).item():
        raise ValueError("Duplikaty klucza stacja-czas")
    counts = frame.group_by("kod_stacji").len().sort("kod_stacji")
    station_counts = list(counts.iter_rows())
    for station, _ in station_counts:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", station):
            raise ValueError(f"Kod stacji wymaga kodowania sciezki Hive: {station!r}")
    mapping, loads = balanced_station_groups(station_counts, ctx["group_count"])
    c_mapping, c_loads = balanced_station_groups(station_counts, ctx["c_group_count"])
    ctx["columns"], ctx["station_groups"] = frame.columns, mapping
    ctx["c_station_groups"] = c_mapping
    ctx["actual_c_group_count"] = len(c_loads)
    ctx["actual_group_count"] = len(loads)
    table = frame.sort(["kod_stacji", "data_czas_utc"]).to_arrow().combine_chunks()
    del frame
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
        for group in range(len(c_loads)):
            folder = c / f"station_group={group}"
            folder.mkdir()
            tables = [station_tables[s] for s in sorted(c_mapping) if c_mapping[s] == group]
            pq.write_table(pa.concat_tables(tables), folder / "part-0.parquet", **options)
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
    save_csv(ctx["report"] / "station_groups_C.csv", [
        dict(kod_stacji=s, station_group=c_mapping[s], rows=n) for s,n in station_counts])
    save_json(ctx["report"] / "layout.json", {
        "columns": ctx["columns"], "station_groups": mapping, "group_rows": loads,
        "A": "flat", "B": "balanced station groups", "C": "3K balanced station groups",
        "c_station_groups": c_mapping, "c_group_rows": c_loads,
        "technical_column": "station_group is excluded from the logical schema",
    })
    static = ctx["data"]/"static_reports"
    static.mkdir(exist_ok=True)
    for name in ("station_groups.csv","station_groups_C.csv","layout.json"):
        shutil.copy2(ctx["report"]/name,static/name)
    configure_readers(ctx)
    return inspect_storage(ctx)


def inspect_storage(ctx):
    file_sets = {"A": [ctx["flat"]], "B": sorted(ctx["grouped"].glob("station_group=*/*.parquet")),
                 "C": sorted(ctx["partitioned"].glob("station_group=*/*.parquet"))}
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
    for variant, expected in [("B",ctx["group_count"]),("C",ctx["c_group_count"])]:
        if next(r["files"] for r in totals if r["variant"] == variant) != expected:
            raise AssertionError(f"Nieprawidlowa liczba partycji {variant}")
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
    readers = {"source":parquet_reader(ctx["source"]),
               **{v:logical_reader(ctx,v) for v in CONFIGS}}
    schemas = {v:[(r[0],r[1]) for r in con.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()]
               for v,reader in readers.items()}
    if any(s != schemas["source"] for s in schemas.values()):
        raise AssertionError("Niezgodne schematy logiczne")
    # Parquet footers: no scan of all data columns.
    counts = {"source":ctx["scaling"]["actual_rows"]}
    for v,paths in {"A":[ctx["flat"]],
                    "B":list(ctx["grouped"].glob("station_group=*/*.parquet")),
                    "C":list(ctx["partitioned"].glob("station_group=*/*.parquet"))}.items():
        total = 0
        for path in paths:
            with pq.ParquetFile(path) as handle:
                total += handle.metadata.num_rows
        counts[v] = total
    if not counts["source"] or len(set(counts.values())) != 1:
        raise AssertionError("Rozne liczby wierszy lub pusty zbior")
    full = ctx["validation_mode"] == "full"
    if full:
        cols = ", ".join(map(identifier,ctx["columns"]))
        for v,reader in readers.items():
            invalid = con.execute(f"""SELECT count(*) FROM (
                SELECT kod_stacji,data_czas_utc FROM {reader} GROUP BY 1,2
                HAVING count(*)<>1 OR kod_stacji IS NULL OR data_czas_utc IS NULL) q""").fetchone()[0]
            if invalid:
                raise AssertionError(f"Niepoprawne klucze {v}")
        for v in CONFIGS:
            for left,right in [(readers["source"],readers[v]),(readers[v],readers["source"])]:
                diff = con.execute(f"SELECT count(*) FROM (SELECT {cols} FROM {left} EXCEPT ALL SELECT {cols} FROM {right}) q").fetchone()[0]
                if diff:
                    raise AssertionError(f"Rozne dane {v}")
        for v,mapping in [("B",ctx["station_groups"]),("C",ctx["c_station_groups"])]:
            cases = " ".join(f"WHEN {literal(s)} THEN {g}" for s,g in sorted(mapping.items()))
            invalid = con.execute(f"SELECT count(*) FROM {ctx['readers'][v]} WHERE station_group IS DISTINCT FROM (CASE kod_stacji {cases} ELSE NULL END)").fetchone()[0]
            if invalid:
                raise AssertionError(f"Niepoprawne przypisanie grupy {v}")
    result = dict(status="OK", mode=ctx["validation_mode"], source_rows=counts["source"],
                  A_rows=counts["A"],B_rows=counts["B"],C_rows=counts["C"],
                  checked_columns=len(ctx["columns"]),full_value_equality_checked=full,
                  key_uniqueness_checked=full, group_assignment_scanned=full,
                  note="Quick: schemas, footer row counts; aggregate result checks follow outside timer")
    if report_name is not None:
        save_json(ctx["report"]/report_name,result)
    if not ctx["reused"]:
        save_json(ctx["data"]/"prepared.json",dict(recipe=ctx["cache_recipe"],scaling=ctx["scaling"],
            columns=ctx["columns"], station_groups=ctx["station_groups"],c_station_groups=ctx["c_station_groups"],
            actual_group_count=ctx["actual_group_count"],actual_c_group_count=ctx["actual_c_group_count"],
            files=cache_files(ctx["data"]),validation_mode=ctx["validation_mode"]))
    check_source(ctx)
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
    sizes = sorted({n for n in ctx["scenario_sizes"] if n < len(stations)})
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
        c_groups = None if selected is None else sorted({ctx["c_station_groups"][s] for s in selected})
        item["selected_c_groups"] = ctx["actual_c_group_count"] if c_groups is None else len(c_groups)
        queries = {
            "A": query(ctx["readers"]["A"], selected),
            "B": query(ctx["readers"]["B"], selected, selected_groups),
            "C": query(ctx["readers"]["C"], selected, c_groups),
        }
        filtered_a = full_a if selected is None else ctx["con"].execute(queries["A"]).fetchall()
        for config, sql in queries.items():
            scope = "all" if selected is None else "selected"
            expected = full_a if scope == "all" else filtered_a
            actual = expected if config == "A" else ctx["con"].execute(sql).fetchall()
            assert_same(expected, actual)
            ctx["queries"][item["id"], config] = sql
            ctx["expected"][item["id"], config] = expected
            (ctx["report"] / f"{item['id']}_{config}.sql").write_text(sql, encoding="utf-8")
            plan = ctx["con"].execute("EXPLAIN " + sql).fetchall()
            (ctx["report"] / f"plan_{item['id']}_{config}.txt").write_text(
                "\n".join(str(r[-1]) for r in plan), encoding="utf-8")
        save_csv(ctx["report"] / f"aggregation_{item['id']}_selected.csv",
                 [dict(zip(RESULT_COLUMNS, row)) for row in filtered_a])
    save_csv(ctx["report"] / "aggregation_all.csv", [dict(zip(RESULT_COLUMNS, r)) for r in full_a])
    save_json(ctx["report"] / "scenarios.json", scenarios)
    save_json(ctx["report"] / "protocol.json", {
        "configs":CONFIGS, "B_groups":ctx["group_count"],"C_groups":ctx["c_group_count"],
        "B_C_filters":"station_group IN (...) AND kod_stacji IN (...)",
        "same_work":"Same selected stations for A/B/C in every scenario",
        "plans":"EXPLAIN only, no additional query execution",
        "validation_mode":ctx["validation_mode"],
        "scenario_sizes":"Count station IDs in current dataset, including copies",
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
        print(f"Powtorzenie {iteration}/{ctx['repeats']}",flush=True)
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
                scope = "all" if item["stations"] is None else "selected"
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
        scope = "all" if scenarios[r["id"]]["stations"] is None else "selected"
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
        pairs = [("A", "B"), ("A", "C"), ("C", "B")]
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
    colors = {"A": "#4C78A8", "B": "#2CA58D", "C": "#E39C37"}
    figures = []
    try:
        fig, ax = plt.subplots(figsize=(14, 6))
        figures.append(fig)
        width = .24
        for j, variant in enumerate(CONFIGS):
            ax.bar([i + (j - 1) * width for i in range(len(scenarios))],
                   [indexed[s["id"], variant]["mean_ms"] for s in scenarios], width=width,
                   yerr=[indexed[s["id"], variant]["std_ms"] for s in scenarios],
                   capsize=3, label=LABELS[variant], color=colors[variant])
        ax.set_xticks(range(len(scenarios)), [s["scenario"] for s in scenarios])
        ax.set_ylabel("Czas [ms], srednia +/- SD")
        ax.set_title(f"A/B/C: B={ctx['group_count']} grup, C={ctx['c_group_count']} grup")
        ax.legend(fontsize=9)
        fig.tight_layout(); fig.savefig(ctx["report"] / "01_mean_times.png", dpi=160)

        fig, gain_ax = plt.subplots(figsize=(14, 5))
        axes = [gain_ax]
        figures.append(fig)
        for variant, offset in [("B", -.18), ("C", .18)]:
            values = [100 * (indexed[s["id"], "A"]["mean_ms"] - indexed[s["id"], variant]["mean_ms"])
                      / indexed[s["id"], "A"]["mean_ms"] for s in scenarios]
            axes[0].bar([i + offset for i in range(len(scenarios))], values, .36,
                        color=colors[variant], label=variant + " wzgledem A")
        axes[0].set_xticks(range(len(scenarios)), [s["scenario"] for s in scenarios], rotation=20)
        axes[0].set_title("Zgodny zakres: A/B/C")
        axes[0].set_ylabel("Zysk czasu [%]"); axes[0].axhline(0, color="black", lw=.7); axes[0].legend()
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
        (f"B ma {ctx['group_count']} grup stacji, C ma {ctx['c_group_count']} grup. "
         "A/B/C obliczaja wynik dla tych samych stacji w kazdym scenariuszu."),
        (f"Walidacja: {ctx['validation_mode']}. Tryb quick nie porownuje wszystkich wartosci "
         "i nie sprawdza unikalnosci kluczy calego zbioru. Zgodnosc agregacji sprawdzono osobno."),
        (f"Ponowne wykorzystanie przygotowanych danych: {ctx['reused']}. "
         "Przygotowanie i walidacja nie wchodza do czasu zapytan."),
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
                         f"filtr B obejmuje {scenario['selected_groups']} z {ctx['actual_group_count']} grup, "
                         f"filtr C {scenario['selected_c_groups']} z {ctx['actual_c_group_count']} grup.")
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
                    reason = ("Mniejsza liczba grup B wydluzyla czas wzgledem wiekszej liczby grup C. "
                              "Mozliwym powodem jest odczyt danych dodatkowych stacji w wybranych grupach; "
                              "oszczednosc liczby plikow nie wystarczyla w tym pomiarze.")
            elif r["time_saved_percent"] > 0:
                reason = ("Partycjonowanie skrocilo czas wzgledem pliku plaskiego dla tych samych stacji. "
                          "Wynik jest zgodny z korzyscia z ograniczenia odczytu, jezeli plan potwierdza pruning. "
                          "Wniosek dotyczy tego zakresu danych.")
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
    lines.append("Nie wyliczamy kosztow chmury ani "
                 "fizycznie odczytanych bajtow na podstawie samego czasu. EXPLAIN pokazuje planowane filtry i pruning, nie pomiar fizycznego odczytu.")
    text = "\n\n".join(lines)
    (ctx["report"] / "interpretation.md").write_text(text, encoding="utf-8")
    return text


def run_experiment(**settings):
    ctx = create_context(**settings)
    stage_times = {}
    def stage(label, function):
        print(label, flush=True)
        start = perf_counter_ns()
        result = function()
        stage_times[label] = (perf_counter_ns()-start)/1e9
        print(f"OK: {label} ({stage_times[label]:.2f} s)",flush=True)
        return result
    try:
        storage = stage("Zapis lub odczyt cache A/B/C",lambda:write_variants(ctx))
        validation = stage("Walidacja " + ctx["validation_mode"],lambda:verify_data(ctx))
        stage("Scenariusze i EXPLAIN",lambda:prepare_scenarios(ctx))
        timings = stage("Benchmark",lambda:run_benchmark(ctx))
        summary = summarize(ctx,timings)
        if ctx["generate_plots"]:
            try:
                figures = make_plots(ctx,timings,summary)
            except ModuleNotFoundError as exc:
                if exc.name != "matplotlib":
                    raise
                print("Brak matplotlib: pomijam wykresy",flush=True)
            else:
                import matplotlib.pyplot as plt
                for fig in figures:
                    plt.close(fig)
        describe_results(ctx,summary,storage)
        save_json(ctx["report"]/"stage_times.json",stage_times)
        return dict(report=ctx["report"],data=ctx["data"],scale_factor=ctx["scale_factor"],
                    scaling=ctx["scaling"],summary=summary,storage=storage["storage"],
                    reused=ctx["reused"],validation=validation,stage_times=stage_times)
    finally:
        ctx["con"].close()
        release_lock(ctx)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="A/B/C partition benchmark")
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--source", type=Path, default=None)
    parser.add_argument("--scale", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--threads", type=int, default=None)
    parser.add_argument("--groups", type=int, default=8)
    parser.add_argument("--row-group-size", type=int, default=122880)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--scenario-sizes", type=int, nargs="+", default=[1, 8, 32])
    parser.add_argument("--reader-mode", choices=["all_files_list", "glob"], default="all_files_list")
    parser.add_argument("--write-statistics", action="store_true")
    parser.add_argument("--memory-limit", default=None)
    parser.add_argument("--small-difference-percent", type=float, default=5.0)
    parser.add_argument("--label", default="benchmark")
    parser.add_argument("--validation",choices=["quick","full"],default="quick")
    parser.add_argument("--no-reuse",action="store_true")
    parser.add_argument("--no-plots",action="store_true")
    args = parser.parse_args()
    result = run_experiment(root=args.root, source_path=args.source, scale_factor=args.scale,
        repeats=args.repeats, warmups=args.warmups, threads=args.threads, group_count=args.groups,
        row_group_size=args.row_group_size, seed=args.seed, scenario_sizes=args.scenario_sizes,
        reader_mode=args.reader_mode, write_statistics=args.write_statistics,
        memory_limit=args.memory_limit, small_difference_percent=args.small_difference_percent,
        run_label=args.label,validation_mode=args.validation,
        reuse_prepared_data=not args.no_reuse,generate_plots=not args.no_plots)
    print(result["summary"])
    print("Raport:", result["report"])


if __name__ == "__main__":
    main()
