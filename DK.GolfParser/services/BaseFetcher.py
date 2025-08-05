from abc import ABC, abstractmethod
import requests
# from datadog import statsd
# import logging
# from Services.rabbitmq import publish


class BaseFetcher(ABC):
    @property
    @abstractmethod
    def url(self) -> str:
        ...

    @property
    @abstractmethod
    def topic_template(self) -> str:
        ...

    @abstractmethod
    def fetch(self) -> list[dict]:
        ...

    @abstractmethod
    def transform(self, item: dict) -> dict:
        ...

    def _safe_request(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
        response = requests.get(self.url, headers=headers, timeout=10)
        response.raise_for_status()
        return response

    def process(self):
        try:
            items = self.fetch()
            for item in items:
                transformed = self.transform(item)
                self._publish(transformed)
        except Exception as e:
            statsd.increment(f"scraper.{self.__class__.__name__.lower()}.failed")
            logging.error(f"{self.__class__.__name__} error: {e}")

    def _publish(self, item: dict):
        topic = self.topic_template.format(**item)
        publish(topic, item)
        statsd.increment(f"scraper.{self.__class__.__name__.lower()}.processed")
        logging.info(f"Published: {topic}")