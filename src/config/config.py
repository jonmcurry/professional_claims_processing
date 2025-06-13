from typing import Any, Dict, List, Optional

try:
    import yaml
except Exception:  # pragma: no cover - fallback simple parser

    class _SimpleYAML:
        @staticmethod
        def safe_load(text: str) -> Dict[str, Any]:
            data: Dict[str, Any] = {}
            section = None
            for raw in text.splitlines():
                if not raw.strip() or raw.lstrip().startswith("#"):
                    continue
                if not raw.startswith(" "):
                    section = raw.rstrip(":")
                    data[section] = {}
                else:
                    key, val = raw.strip().split(":", 1)
                    val = val.strip()
                    if val.isdigit():
                        val = int(val)
                    else:
                        try:
                            val = float(val)
                        except ValueError:
                            pass
                    data[section][key] = val
            return data

    yaml = _SimpleYAML()
from pathlib import Path
from dataclasses import dataclass


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
class SecurityConfig:
    api_key: str = ""
    encryption_key: str = ""


@dataclass
class CacheConfig:
    redis_url: Optional[str] = None
    warm_rvu_codes: List[str] | None = None


@dataclass
class AppConfig:
    postgres: PostgresConfig
    sqlserver: SQLServerConfig
    processing: ProcessingConfig
    security: SecurityConfig
    cache: CacheConfig


def load_config(path: str = "config.yaml") -> AppConfig:
    data: Dict[str, Any] = yaml.safe_load(Path(path).read_text())
    pg = PostgresConfig(**data.get("postgresql", {}))
    sql = SQLServerConfig(**data.get("sqlserver", {}))
    proc = ProcessingConfig(**data.get("processing", {}))
    sec = SecurityConfig(**data.get("security", {}))
    cache_cfg = CacheConfig(**data.get("cache", {}))
    return AppConfig(
        postgres=pg, sqlserver=sql, processing=proc, security=sec, cache=cache_cfg
    )
