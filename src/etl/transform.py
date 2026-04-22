"""Transforms: a small registry of DataFrame-in / DataFrame-out operations.

Each transform is a function `(df, **kwargs) -> df`. Register new transforms with
the `@register("name")` decorator; reference them in YAML via `type: <name>`.
"""

from __future__ import annotations

from typing import Callable

import pandas as pd


_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {}


def register(name: str):
    """Decorator that adds a transform to the global registry."""
    def decorator(fn: Callable[..., pd.DataFrame]) -> Callable[..., pd.DataFrame]:
        if name in _REGISTRY:
            raise ValueError(f"Transform {name!r} already registered")
        _REGISTRY[name] = fn
        return fn
    return decorator


def apply(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Apply a single transform described by a config dict."""
    cfg = dict(config)
    kind = cfg.pop("type")
    if kind not in _REGISTRY:
        raise ValueError(f"Unknown transform {kind!r}. Known: {sorted(_REGISTRY)}")
    return _REGISTRY[kind](df, **cfg)


# ---- built-in transforms ----------------------------------------------------


@register("drop_nulls")
def drop_nulls(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    """Drop rows with nulls in the given columns (or any column if omitted)."""
    return df.dropna(subset=columns).reset_index(drop=True)


@register("rename_columns")
def rename_columns(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    return df.rename(columns=mapping)


@register("filter_rows")
def filter_rows(df: pd.DataFrame, query: str) -> pd.DataFrame:
    """Filter rows by a pandas query string, e.g. `amount > 0`."""
    return df.query(query).reset_index(drop=True)


@register("derive_column")
def derive_column(df: pd.DataFrame, name: str, expr: str) -> pd.DataFrame:
    """Add a new column from a pandas eval expression, e.g. `amount * quantity`."""
    df = df.copy()
    df[name] = df.eval(expr)
    return df


@register("deduplicate")
def deduplicate(df: pd.DataFrame, subset: list[str] | None = None) -> pd.DataFrame:
    return df.drop_duplicates(subset=subset).reset_index(drop=True)


def list_transforms() -> list[str]:
    """For docs / CLI introspection."""
    return sorted(_REGISTRY)
