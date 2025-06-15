from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Optional

from ..alerting import EmailNotifier
from ..config.config import AppConfig, load_config
from ..monitoring.metrics import metrics


class RecoveryManager:
    """Attempt to recover from backup mode by reconnecting services."""

    def __init__(
        self,
        pipeline: "ClaimsPipeline",
        cfg: Optional[AppConfig] = None,
        *,
        check_interval: float = 10.0,
        max_attempts: int = 3,
    ) -> None:
        self.pipeline = pipeline
        self.cfg = cfg or pipeline.cfg
        self.check_interval = check_interval
        self.max_attempts = max_attempts
        self._task: Optional[asyncio.Task] = None
        self._attempts = 0
        self.logger = logging.getLogger("claims_processor")

    def start(self) -> None:
        if not self._task:
            loop = asyncio.get_event_loop()
            self._task = loop.create_task(self._loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self.check_interval)
            if self.pipeline.mode == "backup":
                recovered = await self.attempt_recovery()
                if recovered:
                    self._attempts = 0
                else:
                    self._attempts += 1
                    if self._attempts >= self.max_attempts:
                        self._notify_failure()
                        self._attempts = 0
            else:
                self._attempts = 0

    async def attempt_recovery(self) -> bool:
        """Run the configured recovery steps."""
        try:
            await asyncio.gather(
                self.pipeline.pg.connect(),
                self.pipeline.sql.connect(),
            )
            await self.pipeline._check_services_health()
            if self.pipeline.mode == "normal":
                await self._flush_local_queue()
                await self._warm_cache()
                metrics.inc("recoveries")
                return True
        except Exception as exc:  # pragma: no cover - best effort
            self.logger.error("Recovery attempt failed: %s", exc)
        return False

    async def _flush_local_queue(self) -> None:
        path = self.pipeline.local_queue_path
        if not path.exists():
            return
        try:
            with path.open("r", encoding="utf-8") as fh:
                claims = [json.loads(l) for l in fh if l.strip()]
            path.unlink(missing_ok=True)
            if claims:
                await self.pipeline.process_claims_batch(claims)
                self.logger.info("Flushed %d queued claims", len(claims))
        except Exception as exc:  # pragma: no cover - best effort
            self.logger.error("Failed to flush local queue: %s", exc)

    async def _warm_cache(self) -> None:
        if (
            self.pipeline.features.enable_cache
            and self.pipeline.cfg.cache.warm_rvu_codes
        ):
            try:
                await self.pipeline.rvu_cache.warm_cache(
                    self.pipeline.cfg.cache.warm_rvu_codes
                )
            except Exception as exc:  # pragma: no cover - best effort
                self.logger.warning("Cache warm-up failed: %s", exc)

    def _notify_failure(self) -> None:
        recipients = self.cfg.monitoring.alerts.email_recipients
        if not recipients:
            return
        notifier = EmailNotifier()
        body = "Automatic recovery attempts failed. Manual intervention required."
        notifier.send(recipients, "Claims Processor Recovery Failed", body)
        self.logger.error("Recovery failed after %d attempts", self.max_attempts)
        metrics.inc("recovery_failures")


async def _run_command(cmd: str) -> None:
    cfg = load_config()
    from ..processing.pipeline import ClaimsPipeline

    pipeline = ClaimsPipeline(cfg)
    manager = RecoveryManager(pipeline, cfg)

    if cmd == "flush":
        await manager._flush_local_queue()
    else:
        await manager.attempt_recovery()


if __name__ == "__main__":  # pragma: no cover - manual utility
    import sys

    command = sys.argv[1] if len(sys.argv) > 1 else "recover"
    asyncio.run(_run_command(command))
