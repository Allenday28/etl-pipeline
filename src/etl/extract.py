"""Extractors: read source data into a pandas DataFrame."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


class Extractor:
    """Base class for all extractors. Subclass and implement `extract`."""

    def extract(self) -> pd.DataFrame:  # pragma: no cover - interface
        raise NotImplementedError


class CSVExtractor(Extractor):
    def __init__(self, path: str, **kwargs):
        self.path = Path(path)
        self.kwargs = kwargs

    def extract(self) -> pd.DataFrame:
        if not self.path.exists():
            raise FileNotFoundError(f"CSV source not found: {self.path}")
        return pd.read_csv(self.path, **self.kwargs)


class JSONExtractor(Extractor):
    def __init__(self, path: str, **kwargs):
        self.path = Path(path)
        self.kwargs = kwargs

    def extract(self) -> pd.DataFrame:
        if not self.path.exists():
            raise FileNotFoundError(f"JSON source not found: {self.path}")
        return pd.read_json(self.path, **self.kwargs)


_EXTRACTORS = {
    "csv": CSVExtractor,
    "json": JSONExtractor,
}


def build(config: dict) -> Extractor:
    """Build an extractor from a config dict like {"type": "csv", "path": "..."}"""
    cfg = dict(config)
    kind = cfg.pop("type")
    if kind not in _EXTRACTORS:
        raise ValueError(f"Unknown extractor type: {kind!r}. Known: {list(_EXTRACTORS)}")
    return _EXTRACTORS[kind](**cfg)
