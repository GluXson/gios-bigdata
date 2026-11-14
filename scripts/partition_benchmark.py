"""Partycjonowanie i benchmark A/B danych silver GIOS."""
from pathlib import Path
from datetime import datetime, timezone
from time import perf_counter_ns
from importlib.metadata import version
import csv
import json
import math
import os
import platform
import random
import statistics
import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import polars as pl

ROOT = Path(__file__).resolve().parents[1]


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def save_csv(path, rows):
    if rows:
        with Path(path).open('w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)


def create_context(root=ROOT, repeats=20, warmups=3, threads=None, write_statistics=False):
    if repeats < 2 or warmups < 0:
        raise ValueError('Wymagane repeats >= 2, warmups >= 0')
    root = Path(root).resolve()
    source = root / 'data/silver/pomiary_2024.parquet'
    if not source.is_file():
        raise FileNotFoundError(f'Najpierw wykonaj 04_silver.ipynb: {source}')
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S_%fZ')
    data = root / 'data/benchmark' / stamp
    report = root / 'reports/partition_benchmark' / stamp
    data.mkdir(parents=True)
    report.mkdir(parents=True)
    threads = min(4, os.cpu_count() or 1) if threads is None else int(threads)
    if threads < 1:
        raise ValueError('threads >= 1')
    con = duckdb.connect()
    con.execute(f'SET threads = {threads}')
    ctx = dict(root=root, source=source, flat=data/'flat.parquet',
               partitioned=data/'partitioned', report=report, data=data,
               repeats=int(repeats), warmups=int(warmups), threads=threads,
               row_group_size=122880, write_statistics=bool(write_statistics), con=con)
    metadata = dict(created_utc=stamp, source=str(source), source_bytes=source.stat().st_size,
                    source_mtime_ns=source.stat().st_mtime_ns,
                    python=platform.python_version(), platform=platform.platform(),
                    processor=platform.processor(), logical_cpus=os.cpu_count(),
                    threads=threads, repeats=repeats, warmups=warmups, seed=42,
                    compression='snappy', write_statistics=bool(write_statistics),
                    row_group_size_limit=122880, cache='warm OS cache; OS cache not cleared',
                    versions={p:version(p) for p in ['duckdb','pyarrow','polars','matplotlib']})
    (report/'environment.json').write_text(json.dumps(metadata,ensure_ascii=False,indent=2),encoding='utf-8')
    return ctx


def write_variants(ctx):
    if ctx['flat'].exists() or ctx['partitioned'].exists():
        raise FileExistsError('Warianty istnieja. Utworz nowy kontekst.')
    source = ds.dataset(ctx['source'],format='parquet')
    missing = {'wskaznik','wartosc','data_czas_utc','czas_usredniania'}-set(source.schema.names)
    if missing:
        raise ValueError(f'Brak kolumn: {missing}')
    if source.count_rows(filter=ds.field('wskaznik').is_null()):
        raise ValueError('NULL w kluczu partycji')
    n=ctx['row_group_size']
    with pq.ParquetWriter(ctx['flat'],source.schema,compression='snappy',
                          write_statistics=ctx['write_statistics']) as writer:
        for batch in source.to_batches(batch_size=n):
            writer.write_batch(batch,row_group_size=n)
    fmt=ds.ParquetFileFormat()
    options=fmt.make_write_options(compression='snappy',write_statistics=ctx['write_statistics'])
    ds.write_dataset(source,base_dir=str(ctx['partitioned']),format=fmt,file_options=options,
        partitioning=ds.partitioning(pa.schema([source.schema.field('wskaznik')]),flavor='hive'),
        max_rows_per_group=n,max_rows_per_file=n*8,preserve_order=True)
    ctx['readers']={
        'A':f"read_parquet({literal(ctx['flat'].as_posix())}, hive_partitioning=false)",
        'B':f"read_parquet({literal((ctx['partitioned']/'**'/'*.parquet').as_posix())}, hive_partitioning=true)"}
    return inspect_storage(ctx)


def inspect_storage(ctx):
    rows=[]
    tree=[str(ctx['partitioned'].relative_to(ctx['root']))+'/']
    for folder in sorted(ctx['partitioned'].iterdir()):
        if not folder.is_dir():
            continue
        tree.append(f'  {folder.name}/')
        for path in sorted(folder.glob('*.parquet')):
            meta=pq.ParquetFile(path).metadata
            rows.append(dict(partition=folder.name,file=path.name,rows=meta.num_rows,
                             bytes=path.stat().st_size,row_groups=meta.num_row_groups))
            tree.append(f'    {path.name} ({meta.num_rows:,} wierszy, {path.stat().st_size:,} B)')
    if not rows:
        raise AssertionError('Brak partycji')
    for path in [ctx['flat'],*ctx['partitioned'].rglob('*.parquet')]:
        meta=pq.ParquetFile(path).metadata
        if any(meta.row_group(g).column(c).compression!='SNAPPY'
               for g in range(meta.num_row_groups) for c in range(meta.num_columns)):
            raise AssertionError(f'Nieprawidlowa kompresja: {path}')
    meta=pq.ParquetFile(ctx['flat']).metadata
    storage=[dict(variant='A',files=1,rows=meta.num_rows,bytes=ctx['flat'].stat().st_size),
             dict(variant='B',files=len(rows),rows=sum(r['rows'] for r in rows),bytes=sum(r['bytes'] for r in rows))]
    tree='\n'.join(tree)
    (ctx['report']/'directory_tree.txt').write_text(tree,encoding='utf-8')
    save_csv(ctx['report']/'partition_files.csv',rows)
    save_csv(ctx['report']/'storage.csv',storage)
    return dict(tree=tree,files=pl.DataFrame(rows),storage=pl.DataFrame(storage))


def assert_same(a,b):
    if len(a)!=len(b):
        raise AssertionError('Rozna liczba wierszy wynikowych')
    for ra,rb in zip(a,b):
        for va,vb in zip(ra,rb):
            equal=(math.isclose(va,vb,rel_tol=1e-9,abs_tol=1e-8)
                   if isinstance(va,float) and isinstance(vb,(int,float)) else va==vb)
            if not equal:
                raise AssertionError(f'Rozne wyniki: {va!r} != {vb!r}')


def verify_data(ctx):
    con=ctx['con']
    schemas={}
    for v,reader in ctx['readers'].items():
        schemas[v]={r[0]:r[1] for r in con.execute(f'DESCRIBE SELECT * FROM {reader}').fetchall()}
    if schemas['A']!=schemas['B']:
        raise AssertionError('Rozne schematy')
    sql="""SELECT wskaznik,czas_usredniania,hour(data_czas_utc),
        count(*),count(wartosc),sum(wartosc),avg(wartosc)
        FROM {reader} GROUP BY 1,2,3 ORDER BY 1,2,3"""
    a=con.execute(sql.format(reader=ctx['readers']['A'])).fetchall()
    b=con.execute(sql.format(reader=ctx['readers']['B'])).fetchall()
    assert_same(a,b)
    counts=[pq.ParquetFile(ctx['source']).metadata.num_rows,
            pq.ParquetFile(ctx['flat']).metadata.num_rows,
            sum(pq.ParquetFile(p).metadata.num_rows for p in ctx['partitioned'].rglob('*.parquet'))]
    if len(set(counts))!=1:
        raise AssertionError('Rozna liczebnosc')
    return dict(status='OK',source_rows=counts[0],A_rows=counts[1],B_rows=counts[2],checked_groups=len(a))


def query(reader,key):
    predicate='' if key is None else f' AND wskaznik={literal(key)}'
    return f"""SELECT wskaznik,hour(data_czas_utc) AS godzina_utc,
        avg(wartosc) AS srednie_stezenie,count(*) AS liczba_pomiarow
        FROM {reader} WHERE czas_usredniania='1g' {predicate}
        GROUP BY 1,2 ORDER BY 1,2"""


def prepare_scenarios(ctx,indicators=None):
    counts=ctx['con'].execute(f"""SELECT wskaznik,count(*) FROM {ctx['readers']['A']}
        WHERE czas_usredniania='1g' GROUP BY 1 ORDER BY count(*) DESC,wskaznik""").fetchall()
    if not counts:
        raise ValueError('Brak danych godzinowych')
    available=dict(counts)
    if indicators is None:
        indices=sorted({0,len(counts)//3,2*len(counts)//3,len(counts)-1})
        selected=[counts[i][0] for i in indices]
        if 'PM2.5' in available and 'PM2.5' not in selected:
            selected[-1]='PM2.5'
    else:
        selected=list(indicators)
    selected=list(dict.fromkeys(selected))
    if not selected or set(selected)-set(available):
        raise ValueError(f'Nieprawidlowa lista wskaznikow: {selected}')
    total=sum(available.values())
    scenarios=[dict(id=f'q{i:02}',scenario=f'wskaznik={k}',indicator=k,
                    hourly_rows=available[k],hourly_share_pct=100*available[k]/total)
               for i,k in enumerate(selected,1)]
    scenarios.append(dict(id='control',scenario='kontrola_bez_filtra',indicator=None,
                          hourly_rows=total,hourly_share_pct=100.0))
    ctx['scenarios']=scenarios
    ctx['queries']={}
    for item in scenarios:
        results={}
        for v,reader in ctx['readers'].items():
            q=query(reader,item['indicator'])
            ctx['queries'][item['id'],v]=q
            results[v]=ctx['con'].execute(q).fetchall()
            (ctx['report']/f"{item['id']}_{v}.sql").write_text(q,encoding='utf-8')
            plan=ctx['con'].execute('EXPLAIN ANALYZE '+q).fetchall()
            (ctx['report']/f"plan_{item['id']}_{v}.txt").write_text('\n'.join(str(r[-1]) for r in plan),encoding='utf-8')
        assert_same(results['A'],results['B'])
        columns=['wskaznik','godzina_utc','srednie_stezenie','liczba_pomiarow']
        save_csv(ctx['report']/f"aggregation_{item['id']}.csv",[dict(zip(columns,r)) for r in results['A']])
    save_csv(ctx['report']/'scenarios.csv',scenarios)
    return pl.DataFrame(scenarios)


def run_benchmark(ctx):
    con=ctx['con']
    for q in ctx['queries'].values():
        for _ in range(ctx['warmups']):
            con.execute(q).fetchall()
    rng=random.Random(42)
    rows=[]
    for iteration in range(1,ctx['repeats']+1):
        tasks=list(ctx['scenarios'])
        rng.shuffle(tasks)
        for item in tasks:
            variants=['A','B']
            rng.shuffle(variants)
            for order,v in enumerate(variants,1):
                start=perf_counter_ns()
                result=con.execute(ctx['queries'][item['id'],v]).fetchall()
                elapsed=(perf_counter_ns()-start)/1_000_000
                rows.append(dict(id=item['id'],scenario=item['scenario'],iteration=iteration,
                                 variant=v,position_in_pair=order,time_ms=elapsed,result_rows=len(result)))
    save_csv(ctx['report']/'timings_raw.csv',rows)
    return pl.DataFrame(rows)


def summarize(ctx,timings):
    raw=timings.to_dicts()
    rows=[]
    for item in ctx['scenarios']:
        row=dict(id=item['id'],scenario=item['scenario'],repeats=ctx['repeats'])
        for v in ['A','B']:
            t=[r['time_ms'] for r in raw if r['id']==item['id'] and r['variant']==v]
            for name,value in [('mean',statistics.mean(t)),('median',statistics.median(t)),
                               ('std',statistics.stdev(t)),('min',min(t)),('max',max(t))]:
                row[f'{v}_{name}_ms']=value
        a,b=row['A_mean_ms'],row['B_mean_ms']
        row.update(time_saved_percent=100*(a-b)/a,speedup_A_over_B=a/b)
        rows.append(row)
    save_csv(ctx['report']/'summary.csv',rows)
    columns=['scenario','repeats','A_mean_ms','B_mean_ms','A_median_ms','B_median_ms',
             'A_std_ms','B_std_ms','time_saved_percent','speedup_A_over_B']
    lines=['| '+' | '.join(columns)+' |','| '+' | '.join(['---']*len(columns))+' |']
    for row in rows:
        lines.append('| '+' | '.join(f'{row[k]:.3f}' if isinstance(row[k],float) else str(row[k]) for k in columns)+' |')
    (ctx['report']/'summary.md').write_text('\n'.join(lines),encoding='utf-8')
    return pl.DataFrame(rows)


def make_plots(ctx,timings,summary):
    import matplotlib.pyplot as plt
    rows=summary.to_dicts()
    raw=timings.to_dicts()
    labels=[r['scenario'] for r in rows]
    x=list(range(len(rows)))
    figures=[]
    fig,ax=plt.subplots(figsize=(12,6))
    for v,offset in [('A',-.18),('B',.18)]:
        ax.bar([i+offset for i in x],[r[f'{v}_mean_ms'] for r in rows],width=.36,
               yerr=[r[f'{v}_std_ms'] for r in rows],capsize=4,label=v+(' - plaski' if v=='A' else ' - partycje'))
    ax.set_xticks(x,labels,rotation=20,ha='right')
    ax.set_ylabel('Czas [ms], srednia +/- SD')
    ax.set_title('Benchmark A/B - rozgrzany cache')
    ax.legend()
    fig.tight_layout()
    fig.savefig(ctx['report']/'01_mean_times.png',dpi=160)
    figures.append(fig)
    fig,ax=plt.subplots(figsize=(12,6))
    values=[r['time_saved_percent'] for r in rows]
    ax.bar(x,values,color=['seagreen' if v>=0 else 'firebrick' for v in values])
    ax.axhline(0,color='black',linewidth=.8)
    ax.set_xticks(x,labels,rotation=20,ha='right')
    ax.set_ylabel('Skrocenie czasu wzgledem A [%]')
    for i,v in enumerate(values):
        ax.annotate(f'{v:.1f}%',(i,v),ha='center',va='bottom' if v>=0 else 'top')
    fig.tight_layout()
    fig.savefig(ctx['report']/'02_time_saved.png',dpi=160)
    figures.append(fig)
    fig,axes=plt.subplots(len(rows),1,figsize=(12,3*len(rows)),squeeze=False)
    for ax,item in zip(axes[:,0],rows):
        for v in ['A','B']:
            data=[r for r in raw if r['id']==item['id'] and r['variant']==v]
            ax.plot([r['iteration'] for r in data],[r['time_ms'] for r in data],marker='.',label=v)
        ax.set_title(item['scenario'])
        ax.set_ylabel('Czas [ms]')
        ax.legend()
    axes[-1,0].set_xlabel('Powtorzenie')
    fig.tight_layout()
    fig.savefig(ctx['report']/'03_repetitions.png',dpi=160)
    figures.append(fig)
    return figures


def main():
    ctx=create_context()
    try:
        storage=write_variants(ctx)
        print(storage['tree'])
        print(verify_data(ctx))
        print(prepare_scenarios(ctx))
        timings=run_benchmark(ctx)
        summary=summarize(ctx,timings)
        figures=make_plots(ctx,timings,summary)
        import matplotlib.pyplot as plt
        for fig in figures:
            plt.close(fig)
        print(summary)
        print('Raport:',ctx['report'])
        return ctx['report']
    finally:
        ctx['con'].close()


if __name__=='__main__':
    main()
