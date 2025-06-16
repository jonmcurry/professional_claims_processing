"""
Updated configuration dataclasses to support dynamic concurrency management.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class ConcurrencyLimits:
    """Concurrency limits configuration."""

    validation: int
    ml: int
    batch_processing: int
    database: int
    total: int


@dataclass
class ConcurrencyThresholds:
    """Resource thresholds for concurrency scaling."""

    low: int
    medium: int
    high: int
    critical: int


@dataclass
class EmergencyThrottleConfig:
    """Emergency throttling configuration."""

    enable: bool = True
    cpu_threshold: int = 95
    memory_threshold: int = 95
    throttle_factor: float = 0.3
    recovery_time: int = 120


@dataclass
class ConcurrencyConfig:
    """Dynamic concurrency management configuration."""

    mode: str = "adaptive"  # conservative, balanced, aggressive, adaptive
    adjustment_interval: int = 15

    # Initial limits
    initial_validation_limit: int = 100
    initial_ml_limit: int = 60
    initial_batch_limit: int = 12
    initial_database_limit: int = 30
    initial_total_limit: int = 200

    # Boundary limits
    min_limits: ConcurrencyLimits = field(
        default_factory=lambda: ConcurrencyLimits(
            validation=15, ml=8, batch_processing=3, database=8, total=35
        )
    )
    max_limits: ConcurrencyLimits = field(
        default_factory=lambda: ConcurrencyLimits(
            validation=300, ml=150, batch_processing=25, database=80, total=500
        )
    )

    # Thresholds
    cpu_thresholds: ConcurrencyThresholds = field(
        default_factory=lambda: ConcurrencyThresholds(
            low=25, medium=55, high=80, critical=92
        )
    )
    memory_thresholds: ConcurrencyThresholds = field(
        default_factory=lambda: ConcurrencyThresholds(
            low=35, medium=65, high=82, critical=92
        )
    )

    # Advanced settings
    enable_performance_learning: bool = True
    enable_predictive_scaling: bool = True
    scaling_aggressiveness: float = 1.0

    # Emergency throttling
    emergency_throttle: EmergencyThrottleConfig = field(
        default_factory=EmergencyThrottleConfig
    )


@dataclass
class AlertConfig:
    """Alerting configuration."""

    high_cpu_threshold: int = 85
    high_memory_threshold: int = 85
    low_throughput_threshold: int = 1000
    high_error_rate_threshold: float = 0.05
    email_recipients: List[str] = field(default_factory=list)


@dataclass
class MonitoringConfig:
    """Resource monitoring configuration."""

    enable_system_monitoring: bool = True
    resource_check_interval: int = 5
    resource_log_interval: int = 60
    performance_history_size: int = 200
    failure_pattern_threshold: int = 20
    alerts: AlertConfig = field(default_factory=AlertConfig)


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    
    failure_threshold: int = 10
    recovery_time: float = 10.0
    enable_auto_recovery: bool = True
    health_check_interval: float = 30.0


@dataclass
class PostgresConfig:
    """PostgreSQL configuration with enhanced connection pooling."""

    host: str
    port: int
    user: str
    password: str
    database: str
    replica_host: Optional[str] = None
    replica_port: Optional[int] = None
    min_pool_size: int = 25
    max_pool_size: int = 150
    threshold_ms: int = 1000
    connection_timeout: float = 30.0
    command_timeout: float = 60.0
    pool_acquire_timeout: float = 15.0
    retries: int = 3
    retry_delay: float = 0.5
    retry_max_delay: float | None = None
    retry_jitter: float = 0.0
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)


@dataclass
class SQLServerConfig:
    """SQL Server configuration with enhanced connection pooling."""

    host: str
    port: int
    user: str
    password: str
    database: str
    pool_size: int = 40
    min_pool_size: int = 25
    max_pool_size: int = 80
    threshold_ms: int = 1000
    retries: int = 3
    retry_delay: float = 0.5
    retry_max_delay: float | None = None
    retry_jitter: float = 0.0


@dataclass
class ProcessingConfig:
    """Processing configuration with enhanced settings."""

    batch_size: int = 1500
    max_workers: int = 8
    conversion_factor: float = 36.04
    insert_workers: int = 4
    fetcher_workers: int = 2


@dataclass
class SecurityConfig:
    """Security configuration."""

    api_key: str = ""
    encryption_key: str = ""


@dataclass
class CacheConfig:
    """Cache configuration."""

    redis_url: Optional[str] = None
    warm_rvu_codes: List[str] = field(default_factory=list)
    predictive_ahead: int = 3


@dataclass
class ModelConfig:
    """ML model configuration."""

    path: str = "models/filter_model.joblib"
    version: str = "2.1.0"
    ab_test_path: Optional[str] = None
    ab_test_ratio: float = 0.1


@dataclass
class LoggingConfig:
    """Logging configuration with enhanced component-level control."""

    level: str = "INFO"
    component_levels: Dict[str, str] = field(default_factory=dict)
    rotate_mb: int = 50
    backup_count: int = 10
    sentry_dsn: Optional[str] = None
    aggregator_host: Optional[str] = None
    aggregator_port: Optional[int] = None


@dataclass
class FeatureFlags:
    """Feature flags for optional functionality."""

    enable_cache: bool = True
    enable_model_monitor: bool = True
    enable_dynamic_concurrency: bool = True
    enable_performance_analytics: bool = True
    enable_predictive_scaling: bool = True


@dataclass
class AppConfig:
    """Complete application configuration."""

    postgres: PostgresConfig
    sqlserver: SQLServerConfig
    processing: ProcessingConfig
    security: SecurityConfig
    cache: CacheConfig
    model: ModelConfig
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    features: FeatureFlags = field(default_factory=FeatureFlags)
    concurrency: ConcurrencyConfig = field(default_factory=ConcurrencyConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)


def _resolve_path(default: str) -> str:
    """Resolve configuration file path."""
    env_path = os.getenv("APP_CONFIG")
    if not env_path:
        env = os.getenv("APP_ENV")
        if env:
            candidate = f"config.{env}.yaml"
            if Path(candidate).exists():
                env_path = candidate
    return env_path or default


def _create_concurrency_config(data: Dict[str, Any]) -> ConcurrencyConfig:
    """Create concurrency configuration from dictionary."""
    concurrency_data = data.get("concurrency", {})

    # Parse limits
    min_limits_data = concurrency_data.get("min_limits", {})
    max_limits_data = concurrency_data.get("max_limits", {})

    min_limits = ConcurrencyLimits(
        validation=min_limits_data.get("validation", 15),
        ml=min_limits_data.get("ml", 8),
        batch_processing=min_limits_data.get("batch_processing", 3),
        database=min_limits_data.get("database", 8),
        total=min_limits_data.get("total", 35),
    )

    max_limits = ConcurrencyLimits(
        validation=max_limits_data.get("validation", 300),
        ml=max_limits_data.get("ml", 150),
        batch_processing=max_limits_data.get("batch_processing", 25),
        database=max_limits_data.get("database", 80),
        total=max_limits_data.get("total", 500),
    )

    # Parse thresholds
    cpu_thresholds_data = concurrency_data.get("cpu_thresholds", {})
    memory_thresholds_data = concurrency_data.get("memory_thresholds", {})

    cpu_thresholds = ConcurrencyThresholds(
        low=cpu_thresholds_data.get("low", 25),
        medium=cpu_thresholds_data.get("medium", 55),
        high=cpu_thresholds_data.get("high", 80),
        critical=cpu_thresholds_data.get("critical", 92),
    )

    memory_thresholds = ConcurrencyThresholds(
        low=memory_thresholds_data.get("low", 35),
        medium=memory_thresholds_data.get("medium", 65),
        high=memory_thresholds_data.get("high", 82),
        critical=memory_thresholds_data.get("critical", 92),
    )

    # Parse emergency throttle
    emergency_data = concurrency_data.get("emergency_throttle", {})
    emergency_throttle = EmergencyThrottleConfig(
        enable=emergency_data.get("enable", True),
        cpu_threshold=emergency_data.get("cpu_threshold", 95),
        memory_threshold=emergency_data.get("memory_threshold", 95),
        throttle_factor=emergency_data.get("throttle_factor", 0.3),
        recovery_time=emergency_data.get("recovery_time", 120),
    )

    return ConcurrencyConfig(
        mode=concurrency_data.get("mode", "adaptive"),
        adjustment_interval=concurrency_data.get("adjustment_interval", 15),
        initial_validation_limit=concurrency_data.get("initial_validation_limit", 100),
        initial_ml_limit=concurrency_data.get("initial_ml_limit", 60),
        initial_batch_limit=concurrency_data.get("initial_batch_limit", 12),
        initial_database_limit=concurrency_data.get("initial_database_limit", 30),
        initial_total_limit=concurrency_data.get("initial_total_limit", 200),
        min_limits=min_limits,
        max_limits=max_limits,
        cpu_thresholds=cpu_thresholds,
        memory_thresholds=memory_thresholds,
        enable_performance_learning=concurrency_data.get(
            "enable_performance_learning", True
        ),
        enable_predictive_scaling=concurrency_data.get(
            "enable_predictive_scaling", True
        ),
        scaling_aggressiveness=concurrency_data.get("scaling_aggressiveness", 1.0),
        emergency_throttle=emergency_throttle,
    )


def _create_monitoring_config(data: Dict[str, Any]) -> MonitoringConfig:
    """Create monitoring configuration from dictionary."""
    monitoring_data = data.get("monitoring", {})
    alerts_data = monitoring_data.get("alerts", {})

    alerts = AlertConfig(
        high_cpu_threshold=alerts_data.get("high_cpu_threshold", 85),
        high_memory_threshold=alerts_data.get("high_memory_threshold", 85),
        low_throughput_threshold=alerts_data.get("low_throughput_threshold", 1000),
        high_error_rate_threshold=alerts_data.get("high_error_rate_threshold", 0.05),
        email_recipients=alerts_data.get("email_recipients", []),
    )

    return MonitoringConfig(
        enable_system_monitoring=monitoring_data.get("enable_system_monitoring", True),
        resource_check_interval=monitoring_data.get("resource_check_interval", 5),
        resource_log_interval=monitoring_data.get("resource_log_interval", 60),
        performance_history_size=monitoring_data.get("performance_history_size", 200),
        failure_pattern_threshold=monitoring_data.get("failure_pattern_threshold", 20),
        alerts=alerts,
    )


def validate_config(cfg: AppConfig) -> None:
    """Validate configuration consistency and required fields."""
    if not cfg.postgres.host:
        raise ValueError("PostgreSQL host is required")
    if not cfg.sqlserver.host:
        raise ValueError("SQL Server host is required")
    if not cfg.security.api_key:
        raise ValueError("API key must be set")
    if not cfg.model.path:
        raise ValueError("Model path must be configured")

    # Validate concurrency settings
    if cfg.concurrency.mode not in [
        "conservative",
        "balanced",
        "aggressive",
        "adaptive",
    ]:
        raise ValueError(f"Invalid concurrency mode: {cfg.concurrency.mode}")

    if cfg.concurrency.adjustment_interval < 5:
        raise ValueError("Concurrency adjustment interval must be at least 5 seconds")

    # Validate that min limits are less than max limits
    if cfg.concurrency.min_limits.validation >= cfg.concurrency.max_limits.validation:
        raise ValueError("Minimum validation limit must be less than maximum")
    if cfg.concurrency.min_limits.ml >= cfg.concurrency.max_limits.ml:
        raise ValueError("Minimum ML limit must be less than maximum")
    if (
        cfg.concurrency.min_limits.batch_processing
        >= cfg.concurrency.max_limits.batch_processing
    ):
        raise ValueError("Minimum batch processing limit must be less than maximum")
    if cfg.concurrency.min_limits.database >= cfg.concurrency.max_limits.database:
        raise ValueError("Minimum database limit must be less than maximum")
    if cfg.concurrency.min_limits.total >= cfg.concurrency.max_limits.total:
        raise ValueError("Minimum total limit must be less than maximum")

    # Validate scaling aggressiveness
    if not 0.1 <= cfg.concurrency.scaling_aggressiveness <= 5.0:
        raise ValueError("Scaling aggressiveness must be between 0.1 and 5.0")

    # Validate emergency throttle factor
    if not 0.1 <= cfg.concurrency.emergency_throttle.throttle_factor <= 1.0:
        raise ValueError("Emergency throttle factor must be between 0.1 and 1.0")


def load_config(path: str = "config.yaml") -> AppConfig:
    """Load configuration from YAML file."""
    path = _resolve_path(path)

    try:
        with open(path, "r") as f:
            data: Dict[str, Any] = yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in configuration file: {e}")

    # Handle environment variable substitution
    data = _substitute_env_vars(data)

    # Handle PostgreSQL configuration with nested circuit_breaker
    pg_data = data.get("postgresql", {}).copy()
    circuit_breaker_data = pg_data.pop("circuit_breaker", {})
    circuit_breaker_config = CircuitBreakerConfig(
        failure_threshold=circuit_breaker_data.get("failure_threshold", 10),
        recovery_time=circuit_breaker_data.get("recovery_time", 10.0),
        enable_auto_recovery=circuit_breaker_data.get("enable_auto_recovery", True),
        health_check_interval=circuit_breaker_data.get("health_check_interval", 30.0),
    )
    pg = PostgresConfig(circuit_breaker=circuit_breaker_config, **pg_data)
    
    # Create other configuration objects
    sql = SQLServerConfig(**data.get("sqlserver", {}))
    proc = ProcessingConfig(**data.get("processing", {}))
    sec = SecurityConfig(**data.get("security", {}))
    cache_cfg = CacheConfig(**data.get("cache", {}))
    model_cfg = ModelConfig(**data.get("model", {}))

    # Create logging config
    logging_data = data.get("logging", {})
    logging_cfg = LoggingConfig(
        level=logging_data.get("level", "INFO"),
        component_levels=logging_data.get("component_levels", {}),
        rotate_mb=logging_data.get("rotate_mb", 50),
        backup_count=logging_data.get("backup_count", 10),
        sentry_dsn=logging_data.get("sentry_dsn"),
        aggregator_host=logging_data.get("aggregator_host"),
        aggregator_port=logging_data.get("aggregator_port"),
    )

    # Create feature flags
    features_data = data.get("features", {})
    feature_cfg = FeatureFlags(
        enable_cache=features_data.get("enable_cache", True),
        enable_model_monitor=features_data.get("enable_model_monitor", True),
        enable_dynamic_concurrency=features_data.get(
            "enable_dynamic_concurrency", True
        ),
        enable_performance_analytics=features_data.get(
            "enable_performance_analytics", True
        ),
        enable_predictive_scaling=features_data.get("enable_predictive_scaling", True),
    )

    # Override with environment variables
    env_cache = os.getenv("ENABLE_CACHE")
    if env_cache is not None:
        feature_cfg.enable_cache = env_cache.lower() == "true"

    env_monitor = os.getenv("ENABLE_MODEL_MONITOR")
    if env_monitor is not None:
        feature_cfg.enable_model_monitor = env_monitor.lower() == "true"

    env_concurrency = os.getenv("ENABLE_DYNAMIC_CONCURRENCY")
    if env_concurrency is not None:
        feature_cfg.enable_dynamic_concurrency = env_concurrency.lower() == "true"

    # Create enhanced configurations
    concurrency_cfg = _create_concurrency_config(data)
    monitoring_cfg = _create_monitoring_config(data)

    # Create final configuration
    cfg = AppConfig(
        postgres=pg,
        sqlserver=sql,
        processing=proc,
        security=sec,
        cache=cache_cfg,
        model=model_cfg,
        logging=logging_cfg,
        features=feature_cfg,
        concurrency=concurrency_cfg,
        monitoring=monitoring_cfg,
    )

    # Validate configuration
    validate_config(cfg)

    return cfg


def _substitute_env_vars(data: Any) -> Any:
    """Recursively substitute environment variables in configuration data."""
    if isinstance(data, dict):
        return {key: _substitute_env_vars(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [_substitute_env_vars(item) for item in data]
    elif isinstance(data, str) and data.startswith("${") and data.endswith("}"):
        # Extract environment variable name
        env_var = data[2:-1]
        default_value = None

        # Handle default values (e.g., ${VAR_NAME:default_value})
        if ":" in env_var:
            env_var, default_value = env_var.split(":", 1)

        # Get environment variable value
        value = os.getenv(env_var, default_value)
        if value is None:
            raise ValueError(
                f"Environment variable '{env_var}' is required but not set"
            )

        # Try to convert to appropriate type
        if value.lower() in ("true", "false"):
            return value.lower() == "true"
        elif value.isdigit():
            return int(value)
        else:
            try:
                return float(value)
            except ValueError:
                return value
    else:
        return data


def create_default_config() -> AppConfig:
    """Create a default configuration for testing or initial setup."""
    return AppConfig(
        postgres=PostgresConfig(
            host="localhost",
            port=5432,
            user="claims_user",
            password="password",
            database="staging_process",
            retries=3,
            retry_delay=0.5,
            retry_max_delay=None,
            retry_jitter=0.0,
        ),
        sqlserver=SQLServerConfig(
            host="localhost",
            port=1433,
            user="claims_user",
            password="password",
            database="smart_pro_claims",
            retries=3,
            retry_delay=0.5,
            retry_max_delay=None,
            retry_jitter=0.0,
        ),
        processing=ProcessingConfig(),
        security=SecurityConfig(
            api_key="test-api-key", encryption_key="test-encryption-key"
        ),
        cache=CacheConfig(),
        model=ModelConfig(),
        logging=LoggingConfig(),
        features=FeatureFlags(),
        concurrency=ConcurrencyConfig(),
        monitoring=MonitoringConfig(),
    )


def save_config(cfg: AppConfig, path: str = "config.yaml") -> None:
    """Save configuration to YAML file."""
    # Convert dataclasses to dictionaries
    config_dict = {
        "postgresql": {
            "host": cfg.postgres.host,
            "port": cfg.postgres.port,
            "user": cfg.postgres.user,
            "password": cfg.postgres.password,
            "database": cfg.postgres.database,
            "replica_host": cfg.postgres.replica_host,
            "replica_port": cfg.postgres.replica_port,
            "min_pool_size": cfg.postgres.min_pool_size,
            "max_pool_size": cfg.postgres.max_pool_size,
            "threshold_ms": cfg.postgres.threshold_ms,
            "retries": cfg.postgres.retries,
            "retry_delay": cfg.postgres.retry_delay,
            "retry_max_delay": cfg.postgres.retry_max_delay,
            "retry_jitter": cfg.postgres.retry_jitter,
            "circuit_breaker": {
                "failure_threshold": cfg.postgres.circuit_breaker.failure_threshold,
                "recovery_time": cfg.postgres.circuit_breaker.recovery_time,
                "enable_auto_recovery": cfg.postgres.circuit_breaker.enable_auto_recovery,
                "health_check_interval": cfg.postgres.circuit_breaker.health_check_interval,
            },
        },
        "sqlserver": {
            "host": cfg.sqlserver.host,
            "port": cfg.sqlserver.port,
            "user": cfg.sqlserver.user,
            "password": cfg.sqlserver.password,
            "database": cfg.sqlserver.database,
            "pool_size": cfg.sqlserver.pool_size,
            "min_pool_size": cfg.sqlserver.min_pool_size,
            "max_pool_size": cfg.sqlserver.max_pool_size,
            "threshold_ms": cfg.sqlserver.threshold_ms,
            "retries": cfg.sqlserver.retries,
            "retry_delay": cfg.sqlserver.retry_delay,
            "retry_max_delay": cfg.sqlserver.retry_max_delay,
            "retry_jitter": cfg.sqlserver.retry_jitter,
        },
        "processing": {
            "batch_size": cfg.processing.batch_size,
            "max_workers": cfg.processing.max_workers,
            "conversion_factor": cfg.processing.conversion_factor,
            "insert_workers": cfg.processing.insert_workers,
            "fetcher_workers": cfg.processing.fetcher_workers,
        },
        "concurrency": {
            "mode": cfg.concurrency.mode,
            "adjustment_interval": cfg.concurrency.adjustment_interval,
            "initial_validation_limit": cfg.concurrency.initial_validation_limit,
            "initial_ml_limit": cfg.concurrency.initial_ml_limit,
            "initial_batch_limit": cfg.concurrency.initial_batch_limit,
            "initial_database_limit": cfg.concurrency.initial_database_limit,
            "initial_total_limit": cfg.concurrency.initial_total_limit,
            "min_limits": {
                "validation": cfg.concurrency.min_limits.validation,
                "ml": cfg.concurrency.min_limits.ml,
                "batch_processing": cfg.concurrency.min_limits.batch_processing,
                "database": cfg.concurrency.min_limits.database,
                "total": cfg.concurrency.min_limits.total,
            },
            "max_limits": {
                "validation": cfg.concurrency.max_limits.validation,
                "ml": cfg.concurrency.max_limits.ml,
                "batch_processing": cfg.concurrency.max_limits.batch_processing,
                "database": cfg.concurrency.max_limits.database,
                "total": cfg.concurrency.max_limits.total,
            },
            "cpu_thresholds": {
                "low": cfg.concurrency.cpu_thresholds.low,
                "medium": cfg.concurrency.cpu_thresholds.medium,
                "high": cfg.concurrency.cpu_thresholds.high,
                "critical": cfg.concurrency.cpu_thresholds.critical,
            },
            "memory_thresholds": {
                "low": cfg.concurrency.memory_thresholds.low,
                "medium": cfg.concurrency.memory_thresholds.medium,
                "high": cfg.concurrency.memory_thresholds.high,
                "critical": cfg.concurrency.memory_thresholds.critical,
            },
            "enable_performance_learning": cfg.concurrency.enable_performance_learning,
            "enable_predictive_scaling": cfg.concurrency.enable_predictive_scaling,
            "scaling_aggressiveness": cfg.concurrency.scaling_aggressiveness,
            "emergency_throttle": {
                "enable": cfg.concurrency.emergency_throttle.enable,
                "cpu_threshold": cfg.concurrency.emergency_throttle.cpu_threshold,
                "memory_threshold": cfg.concurrency.emergency_throttle.memory_threshold,
                "throttle_factor": cfg.concurrency.emergency_throttle.throttle_factor,
                "recovery_time": cfg.concurrency.emergency_throttle.recovery_time,
            },
        },
        "monitoring": {
            "enable_system_monitoring": cfg.monitoring.enable_system_monitoring,
            "resource_check_interval": cfg.monitoring.resource_check_interval,
            "resource_log_interval": cfg.monitoring.resource_log_interval,
            "performance_history_size": cfg.monitoring.performance_history_size,
            "failure_pattern_threshold": cfg.monitoring.failure_pattern_threshold,
            "alerts": {
                "high_cpu_threshold": cfg.monitoring.alerts.high_cpu_threshold,
                "high_memory_threshold": cfg.monitoring.alerts.high_memory_threshold,
                "low_throughput_threshold": cfg.monitoring.alerts.low_throughput_threshold,
                "high_error_rate_threshold": cfg.monitoring.alerts.high_error_rate_threshold,
                "email_recipients": cfg.monitoring.alerts.email_recipients,
            },
        },
        "security": {
            "api_key": cfg.security.api_key,
            "encryption_key": cfg.security.encryption_key,
        },
        "cache": {
            "redis_url": cfg.cache.redis_url,
            "warm_rvu_codes": cfg.cache.warm_rvu_codes,
            "predictive_ahead": cfg.cache.predictive_ahead,
        },
        "model": {
            "path": cfg.model.path,
            "version": cfg.model.version,
            "ab_test_path": cfg.model.ab_test_path,
            "ab_test_ratio": cfg.model.ab_test_ratio,
        },
        "logging": {
            "level": cfg.logging.level,
            "component_levels": cfg.logging.component_levels,
            "rotate_mb": cfg.logging.rotate_mb,
            "backup_count": cfg.logging.backup_count,
            "sentry_dsn": cfg.logging.sentry_dsn,
            "aggregator_host": cfg.logging.aggregator_host,
            "aggregator_port": cfg.logging.aggregator_port,
        },
        "features": {
            "enable_cache": cfg.features.enable_cache,
            "enable_model_monitor": cfg.features.enable_model_monitor,
            "enable_dynamic_concurrency": cfg.features.enable_dynamic_concurrency,
            "enable_performance_analytics": cfg.features.enable_performance_analytics,
            "enable_predictive_scaling": cfg.features.enable_predictive_scaling,
        },
    }

    with open(path, "w") as f:
        yaml.dump(config_dict, f, default_flow_style=False, indent=2)