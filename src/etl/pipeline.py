"""Pipeline: orchestrate extract → transform → load with a run report."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import yaml

from etl import extract as extract_mod
from etl import load as load_mod
from etl import transform as transform_mod


@dataclass
class StageReport:
    name: str
    rows_in: int
    rows_out: int

    @property
    def delta(self) -> int:
        return self.rows_out - self.rows_in


@dataclass
class RunReport:
    pipeline: str
    stages: list[StageReport] = field(default_factory=list)
    rows_loaded: int = 0

    def format(self) -> str:
        lines = [f"pipeline: {self.pipeline}"]
        for s in self.stages:
            sign = f"{'+' if s.delta >= 0 else ''}{s.delta}"
            lines.append(f"  [{s.name}] {s.rows_in} → {s.rows_out} ({sign})")
        lines.append(f"  loaded: {self.rows_loaded}")
        return "\n".join(lines)


class Pipeline:
    """An ETL pipeline described by a config dict."""

    def __init__(self, config: dict):
        self.config = config
        self.name = config.get("name", "pipeline")

    def run(self) -> RunReport:
        report = RunReport(pipeline=self.name)

        extractor = extract_mod.build(self.config["extract"])
        df = extractor.extract()
        report.stages.append(StageReport("extract", 0, len(df)))

        for t_cfg in self.config.get("transforms", []):
            before = len(df)
            df = transform_mod.apply(df, t_cfg)
            report.stages.append(StageReport(f"transform::{t_cfg['type']}", before, len(df)))

        loader = load_mod.build(self.config["load"])
        rows = loader.load(df)
        report.rows_loaded = rows
        return report


def run_config(path: str | Path) -> RunReport:
    """Load a YAML config from disk and run it."""
    with open(path) as f:
        cfg = yaml.safe_load(f)
    return Pipeline(cfg).run()
