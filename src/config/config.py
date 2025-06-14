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
import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class PostgresConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    replica_host: Optional[str] = None
    replica_port: Optional[int] = None
    min_pool_size: int = 20
    max_pool_size: int = 100


@dataclass
class SQLServerConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    pool_size: int = 20
    min_pool_size: int = 20
    max_pool_size: int = 40


@dataclass
class ProcessingConfig:
    batch_size: int = 1000
    max_workers: int = 4
    conversion_factor: float = 36.04
    insert_workers: int = 2


@dataclass
class SecurityConfig:
    api_key: str = ""
    encryption_key: str = ""


@dataclass
class CacheConfig:
    redis_url: Optional[str] = None
    warm_rvu_codes: List[str] | None = None
    predictive_ahead: int = 2


@dataclass
class ModelConfig:
    path: str = "model.joblib"
    version: str = "1"
    ab_test_path: Optional[str] = None
    ab_test_ratio: float = 0.5


@dataclass
class LoggingConfig:
    level: str = "INFO"
    aggregator_host: Optional[str] = None
    aggregator_port: Optional[int] = None
    component_levels: Dict[str, str] | None = None
    rotate_mb: int = 10
    backup_count: int = 5
    sentry_dsn: Optional[str] = None


@dataclass
class FeatureFlags:
    """Enable or disable optional features."""

    enable_cache: bool = True
    enable_model_monitor: bool = True


@dataclass
class AppConfig:
    postgres: PostgresConfig
    sqlserver: SQLServerConfig
    processing: ProcessingConfig
    security: SecurityConfig
    cache: CacheConfig
    model: ModelConfig
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    features: FeatureFlags = field(default_factory=FeatureFlags)


def _resolve_path(default: str) -> str:
    env_path = os.getenv("APP_CONFIG")
    if not env_path:
        env = os.getenv("APP_ENV")
        if env:
            candidate = f"config.{env}.yaml"
            if Path(candidate).exists():
                env_path = candidate
    return env_path or default


def validate_config(cfg: AppConfig) -> None:
    if not cfg.postgres.host:
        raise ValueError("PostgreSQL host is required")
    if not cfg.sqlserver.host:
        raise ValueError("SQL Server host is required")
    if not cfg.security.api_key:
        raise ValueError("API key must be set")
    if not cfg.model.path:
        raise ValueError("Model path must be configured")


def load_config(path: str = "config.yaml") -> AppConfig:
    path = _resolve_path(path)
    data: Dict[str, Any] = yaml.safe_load(Path(path).read_text())
    pg = PostgresConfig(**data.get("postgresql", {}))
    sql = SQLServerConfig(**data.get("sqlserver", {}))
    proc = ProcessingConfig(**data.get("processing", {}))
    sec = SecurityConfig(**data.get("security", {}))
    cache_cfg = CacheConfig(**data.get("cache", {}))
    model_cfg = ModelConfig(**data.get("model", {}))
    logging_cfg = LoggingConfig(**data.get("logging", {}))
    feature_cfg = FeatureFlags(**data.get("features", {}))
    env_cache = os.getenv("ENABLE_CACHE")
    if env_cache is not None:
        feature_cfg.enable_cache = env_cache.lower() == "true"
    env_monitor = os.getenv("ENABLE_MODEL_MONITOR")
    if env_monitor is not None:
        feature_cfg.enable_model_monitor = env_monitor.lower() == "true"
    cfg = AppConfig(
        postgres=pg,
        sqlserver=sql,
        processing=proc,
        security=sec,
        cache=cache_cfg,
        model=model_cfg,
        logging=logging_cfg,
        features=feature_cfg,
    )
    validate_config(cfg)
    return cfg
