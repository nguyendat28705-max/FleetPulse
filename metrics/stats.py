"""Lớp phát metrics dùng chung qua DogStatsD, có công tắc bật/tắt METRICS_ENABLED."""

from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv

load_dotenv()

METRICS_ENABLED = os.getenv("METRICS_ENABLED", "false").lower() in ("true", "1", "yes")
DD_AGENT_HOST = os.getenv("DD_AGENT_HOST", "localhost")
DD_AGENT_PORT = int(os.getenv("DD_AGENT_PORT", "8125"))


class MetricsClient:
    """Client phát metric an toàn, không ngắt ứng dụng nếu không có Datadog."""

    def __init__(self) -> None:
        self.enabled = METRICS_ENABLED
        self._statsd: Any = None
        if self.enabled:
            try:
                from datadog import DogStatsd
                self._statsd = DogStatsd(host=DD_AGENT_HOST, port=DD_AGENT_PORT, namespace="fleetpulse")
                print(f"[OK] Đã kích hoạt Datadog DogStatsD tại {DD_AGENT_HOST}:{DD_AGENT_PORT}")
            except Exception as e:
                print(f"[!] Không thể khởi tạo Datadog DogStatsd ({e}). Chuyển về chế độ tắt.")
                self.enabled = False

    def gauge(self, metric: str, value: float, tags: list[str] | None = None) -> None:
        if self.enabled and self._statsd:
            try:
                self._statsd.gauge(metric, value, tags=tags)
            except Exception:
                pass

    def increment(self, metric: str, value: int = 1, tags: list[str] | None = None) -> None:
        if self.enabled and self._statsd:
            try:
                self._statsd.increment(metric, value, tags=tags)
            except Exception:
                pass

    def timing(self, metric: str, value: float, tags: list[str] | None = None) -> None:
        if self.enabled and self._statsd:
            try:
                self._statsd.timing(metric, value, tags=tags)
            except Exception:
                pass


metrics = MetricsClient()
