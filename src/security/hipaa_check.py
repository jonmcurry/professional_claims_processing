from __future__ import annotations

"""HIPAA configuration validation helpers."""

from ..config.config import AppConfig, load_config


def check_hipaa_compliance(cfg: AppConfig) -> None:
    """Validate basic HIPAA related settings.

    Raises
    ------
    ValueError
        If required settings are not properly configured.
    """
    if not cfg.security.encryption_key:
        raise ValueError("Encryption must be enabled with a non-empty key")

    if cfg.logging.rotate_mb <= 0 or cfg.logging.backup_count <= 0:
        raise ValueError("Audit logging must be active with log rotation enabled")


def main() -> None:
    cfg = load_config()
    check_hipaa_compliance(cfg)


if __name__ == "__main__":  # pragma: no cover - manual execution
    main()
