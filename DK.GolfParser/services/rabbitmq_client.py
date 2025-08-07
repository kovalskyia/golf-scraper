import json
import aio_pika
import structlog

from typing import Dict, Any, Optional
from services.config import Config
from services.metrics import metrics

logger = structlog.get_logger(__name__)


class RabbitMQClient:

    def __init__(self, config: Config):
        self.config = config
        self.connection = None
        self.channel = None
        self.exchange = None
        self._connection_string = None

    @property
    def connection_string(self) -> str:
        if not self._connection_string:
            self._connection_string = (
                f"amqp://{self.config.rabbitmq_user}:{self.config.rabbitmq_password}"
                f"@{self.config.rabbitmq_host}:{self.config.rabbitmq_port}"
                f"/{self.config.rabbitmq_vhost}"
            )
        return self._connection_string

    async def connect(self) -> None:
        try:
            logger.info(
                "Connecting to RabbitMQ",
                host=self.config.rabbitmq_host,
                port=self.config.rabbitmq_port,
            )

            self.connection = await aio_pika.connect_robust(self.connection_string)
            self.channel = await self.connection.channel()

            # Declare exchange for topic-based routing
            self.exchange = await self.channel.declare_exchange(
                "golf_data", aio_pika.ExchangeType.TOPIC, durable=True
            )

            logger.info("Successfully connected to RabbitMQ")

        except Exception as e:
            logger.error("Failed to connect to RabbitMQ", error=str(e))
            raise

    async def disconnect(self) -> None:
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info("Disconnected from RabbitMQ")

    async def publish_message(
        self, topic: str, data: Dict[str, Any], player_id: Optional[str] = None
    ) -> bool:
        try:
            with metrics.timer("golf_scraper.rabbitmq_publish_latency"):
                message = aio_pika.Message(
                    body=json.dumps(data, default=str).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    content_type="application/json",
                )

                await self.exchange.publish(message, routing_key=topic)

                metrics.record_publish_success(topic, player_id)
                logger.info(
                    "Message published successfully", topic=topic, player_id=player_id
                )
                return True

        except Exception as e:
            error_type = type(e).__name__
            metrics.record_publish_error(topic, error_type, player_id)
            logger.error(
                "Failed to publish message",
                topic=topic,
                error=str(e),
                player_id=player_id,
            )
            return False

    async def publish_endpoint(
        self, topic_key: str, data: Dict[str, Any], **kwargs
    ) -> bool:
        if topic_key not in self.config.topics:
            logger.error(f"Unknown topic config key: {topic_key}")
            return False

        topic = self.config.topics[topic_key]
        if kwargs:
            try:
                topic = topic.format(**kwargs)
            except KeyError as e:
                logger.error(f"Missing required parameter for topic {topic_key}: {e}")
                return False

        # Extract player_id from data if available, otherwise from kwargs
        player_id = kwargs.get("player_id") or data.get("player_id")

        return await self.publish_message(topic, data, player_id)

    async def publish_entrylist(self, player_data: Dict[str, Any]) -> bool:
        return await self.publish_endpoint(
            "entrylist", player_data, player_id=player_data["player_id"]
        )

    async def publish_teetimes(self, player_data: Dict[str, Any]) -> bool:
        return await self.publish_endpoint(
            "teetimes", player_data, player_id=player_data["player_id"]
        )

    async def publish_leaderboard(self, leaderboard_data: list) -> bool:
        return await self.publish_endpoint("leaderboard", {"players": leaderboard_data})

    async def publish_shots(self, shots_data: Dict[str, Any]) -> bool:
        return await self.publish_endpoint(
            "shots", shots_data, player_id=shots_data["player_id"]
        )
