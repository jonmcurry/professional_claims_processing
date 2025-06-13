import yaml
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class PostgresConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass
class SQLServerConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass
class ProcessingConfig:
    batch_size: int = 1000
    max_workers: int = 4
    conversion_factor: float = 36.04


@dataclass
class AppConfig:
    postgres: PostgresConfig
    sqlserver: SQLServerConfig
    processing: ProcessingConfig


def load_config(path: str = "config.yaml") -> AppConfig:
    data: Dict[str, Any] = yaml.safe_load(Path(path).read_text())
    pg = PostgresConfig(**data.get("postgresql", {}))
    sql = SQLServerConfig(**data.get("sqlserver", {}))
    proc = ProcessingConfig(**data.get("processing", {}))
    return AppConfig(postgres=pg, sqlserver=sql, processing=proc)
