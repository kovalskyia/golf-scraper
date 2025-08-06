import time
import structlog

from contextlib import contextmanager
from typing import Dict, Optional


logger = structlog.get_logger(__name__)


class MetricsCollector:

    def __init__(self):
        self.metrics = {}

    def increment_counter(
        self, metric_name: str, value: int = 1, tags: Optional[Dict[str, str]] = None
    ):
        tags = tags or {}
        tag_str = ",".join([f"{k}:{v}" for k, v in tags.items()])

        logger.info(
            "metric",
            metric_name=metric_name,
            value=value,
            metric_type="counter",
            tags=tag_str,
        )

    def record_gauge(
        self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None
    ):
        tags = tags or {}
        tag_str = ",".join([f"{k}:{v}" for k, v in tags.items()])

        logger.info(
            "metric",
            metric_name=metric_name,
            value=value,
            metric_type="gauge",
            tags=tag_str,
        )

    def record_histogram(
        self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None
    ):
        tags = tags or {}
        tag_str = ",".join([f"{k}:{v}" for k, v in tags.items()])

        logger.info(
            "metric",
            metric_name=metric_name,
            value=value,
            metric_type="histogram",
            tags=tag_str,
        )

    @contextmanager
    def timer(self, metric_name: str, tags: Optional[Dict[str, str]] = None):
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.record_histogram(metric_name, duration, tags)

    def record_publish_success(self, topic: str, player_id: Optional[str] = None):
        tags = {"topic": topic}
        if player_id:
            tags["player_id"] = player_id

        self.increment_counter("golf_scraper.messages_published", tags=tags)

    def record_publish_error(
        self, topic: str, error_type: str, player_id: Optional[str] = None
    ):
        tags = {"topic": topic, "error_type": error_type}
        if player_id:
            tags["player_id"] = player_id

        self.increment_counter("golf_scraper.publish_errors", tags=tags)

    def record_fetch_latency(self, endpoint: str, duration: float):
        self.record_histogram(
            "golf_scraper.fetch_latency", duration, {"endpoint": endpoint}
        )

    def record_parse_error(self, endpoint: str, error_type: str):
        self.increment_counter(
            "golf_scraper.parse_errors",
            tags={"endpoint": endpoint, "error_type": error_type},
        )

    def record_rabbitmq_latency(self, duration: float):
        self.record_histogram("golf_scraper.rabbitmq_publish_latency", duration)

    def record_shots_per_hole(self, round_num: int, hole_num: int, shot_count: int):
        self.record_gauge(
            "golf_scraper.shots_per_hole",
            shot_count,
            {"round": str(round_num), "hole": str(hole_num)},
        )


# Global metrics collector instance
metrics = MetricsCollector()
