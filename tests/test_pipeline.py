"""Unit tests for the ETL pipeline. Run with `pytest -q`."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from etl import transform as transform_mod
from etl.pipeline import Pipeline


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "order_id": [1, 2, 2, 3, 4],
        "customer_id": ["A", "B", "B", None, "D"],
        "amount": [10.0, 5.0, 5.0, 7.0, 3.0],
        "quantity": [1, 2, 2, 1, 3],
    })


def test_drop_nulls(sample_df):
    out = transform_mod.apply(sample_df, {"type": "drop_nulls", "columns": ["customer_id"]})
    assert len(out) == 4
    assert out["customer_id"].isna().sum() == 0


def test_deduplicate(sample_df):
    out = transform_mod.apply(sample_df, {"type": "deduplicate", "subset": ["order_id"]})
    assert len(out) == 4


def test_derive_column(sample_df):
    out = transform_mod.apply(
        sample_df, {"type": "derive_column", "name": "total", "expr": "amount * quantity"}
    )
    assert "total" in out.columns
    assert out["total"].iloc[0] == 10.0


def test_filter_rows(sample_df):
    out = transform_mod.apply(sample_df, {"type": "filter_rows", "query": "amount > 4"})
    assert (out["amount"] > 4).all()


def test_rename_columns(sample_df):
    out = transform_mod.apply(sample_df, {"type": "rename_columns", "mapping": {"amount": "amt"}})
    assert "amt" in out.columns and "amount" not in out.columns


def test_unknown_transform_raises(sample_df):
    with pytest.raises(ValueError, match="Unknown transform"):
        transform_mod.apply(sample_df, {"type": "does_not_exist"})


def test_full_pipeline_end_to_end(tmp_path):
    csv_path = tmp_path / "in.csv"
    pd.DataFrame({
        "order_id": [1, 2, 2, 3],
        "cust": ["A", "B", "B", None],
        "amt": [10.0, 5.0, 5.0, 7.0],
        "quantity": [1, 2, 2, 1],
    }).to_csv(csv_path, index=False)

    db_path = tmp_path / "out.sqlite"

    config = {
        "name": "test",
        "extract": {"type": "csv", "path": str(csv_path)},
        "transforms": [
            {"type": "drop_nulls", "columns": ["cust"]},
            {"type": "rename_columns", "mapping": {"cust": "customer_id", "amt": "amount"}},
            {"type": "derive_column", "name": "total", "expr": "amount * quantity"},
            {"type": "deduplicate", "subset": ["order_id"]},
        ],
        "load": {"type": "sqlite", "path": str(db_path), "table": "clean", "if_exists": "replace"},
    }

    report = Pipeline(config).run()
    assert report.rows_loaded == 2  # after null drop + dedupe
    assert db_path.exists()

    # Read back to confirm load worked
    import sqlite3
    with sqlite3.connect(db_path) as conn:
        out = pd.read_sql("select * from clean", conn)
    assert list(out.columns) == ["order_id", "customer_id", "amount", "quantity", "total"]
