"""A small, configurable ETL framework driven by YAML."""

from etl.pipeline import Pipeline, run_config

__version__ = "0.1.0"
__all__ = ["Pipeline", "run_config"]
