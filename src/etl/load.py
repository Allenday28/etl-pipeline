"""Loaders: write a DataFrame to a target."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


class Loader:
    """Base class for loaders. Subclass and implement `load`."""

    def load(self, df: pd.DataFrame) -> int:  # pragma: no cover - interface
        raise NotImplementedError


class SQLiteLoader(Loader):
    def __init__(self, path: str, table: str, if_exists: str = "replace"):
        self.path = Path(path)
        self.table = table
        self.if_exists = if_exists

    def load(self, df: pd.DataFrame) -> int:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as conn:
            df.to_sql(self.table, conn, if_exists=self.if_exists, index=False)
        return len(df)


class CSVLoader(Loader):
    def __init__(self, path: str, **kwargs):
        self.path = Path(path)
        self.kwargs = kwargs

    def load(self, df: pd.DataFrame) -> int:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.path, index=False, **self.kwargs)
        return len(df)


_LOADERS = {
    "sqlite": SQLiteLoader,
    "csv": CSVLoader,
}


def build(config: dict) -> Loader:
    cfg = dict(config)
    kind = cfg.pop("type")
    if kind not in _LOADERS:
        raise ValueError(f"Unknown loader type: {kind!r}. Known: {list(_LOADERS)}")
    return _LOADERS[kind](**cfg)
