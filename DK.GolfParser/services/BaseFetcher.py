from abc import ABC, abstractmethod
import requests
# from datadog import statsd
import logging
# from rabbitmq import publish


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
    def transform(self, item: dict) -> dict:
        ...

    def fetch(self) -> list[dict]:
        logging.info(f"Fetching from: {self.url}")
        headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/115.0 Safari/537.36"
}
        response = requests.get(self.url,headers=headers, timeout=100)
        response.raise_for_status()
        return response.json().get("players", [])

    def publish(self, transformed: dict):
        topic = self.topic_template.format(**transformed)
        publish(topic, transformed)
        statsd.increment(f"scraper.{self.__class__.__name__.lower()}.processed")
        logging.info(f"Published: {topic}")

    def process(self):
        try:
            items = self.fetch()
            for item in items:
                transformed = self.transform(item)
                # self.publish(transformed)
        except Exception as e:
            # statsd.increment(f"scraper.{self.__class__.__name__.lower()}.failed")
            logging.error(f"{self.__class__.__name__} error: {e}")
