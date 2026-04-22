# etl-pipeline

A configurable, testable **Extract / Transform / Load** framework driven by YAML. Built to make the common case (CSV in, SQLite out, some cleaning in between) a one-file config, while keeping the extension points explicit for when you need more.

## Why

Most ETL jobs start as a one-off script and calcify into something nobody wants to touch. This project is a deliberately small framework that separates the three stages clearly, logs what each stage did, and lets you add new extractors, transforms, and loaders without rewriting the pipeline.

## Features

- **Declarative pipelines** — describe a job in a single YAML file
- **Extractors** — CSV and JSON out of the box; add your own by subclassing `Extractor`
- **Transforms** — `filter_rows`, `rename_columns`, `derive_column`, `drop_nulls`, `deduplicate`
- **Loaders** — SQLite and CSV out of the box
- **Run report** — row counts in/out per stage; makes regressions visible
- **CLI** — `python -m etl run config.yaml`

## Quick start

```bash
# from the project root
pip install -r requirements.txt
python -m etl run examples/sales.yaml
```

Expected output:

```
[extract] read 100 rows from examples/data/raw_sales.csv
[transform] drop_nulls      → 98 rows (-2)
[transform] rename_columns  → 98 rows
[transform] derive_column   → 98 rows (added total)
[transform] deduplicate     → 96 rows (-2)
[load] wrote 96 rows to output/sales.sqlite :: sales_clean
```

## Example config

```yaml
# examples/sales.yaml
name: sales-cleanup
extract:
  type: csv
  path: examples/data/raw_sales.csv
transforms:
  - type: drop_nulls
    columns: [order_id, customer_id]
  - type: rename_columns
    mapping: { cust: customer_id, amt: amount }
  - type: derive_column
    name: total
    expr: "amount * quantity"
  - type: deduplicate
    subset: [order_id]
load:
  type: sqlite
  path: output/sales.sqlite
  table: sales_clean
  if_exists: replace
```

## Project layout

```
etl-pipeline/
├── src/etl/
│   ├── __init__.py
│   ├── pipeline.py      # orchestrates extract → transform → load
│   ├── extract.py       # CSV / JSON extractors
│   ├── transform.py     # transform registry
│   ├── load.py          # SQLite / CSV loaders
│   └── cli.py           # python -m etl run config.yaml
├── tests/
│   └── test_pipeline.py
├── examples/
│   ├── sales.yaml
│   └── data/raw_sales.csv
├── requirements.txt
├── LICENSE
└── .gitignore
```

## Running the tests

```bash
pytest -q
```

## Extending

Add a new transform by writing a function that takes a DataFrame and kwargs, then registering it:

```python
from etl.transform import register

@register("z_score")
def z_score(df, column, out):
    df[out] = (df[column] - df[column].mean()) / df[column].std()
    return df
```

Reference it in YAML as `type: z_score`.

## Tech

Python 3.10+ · pandas · PyYAML · sqlite3 (stdlib) · pytest

## License

MIT
