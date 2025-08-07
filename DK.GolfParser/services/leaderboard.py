import structlog

from services.api_client import APIClient
from services.data_processors import DataProcessor
from services.config import Config
from services.rabbitmq_client import RabbitMQClient

logger = structlog.get_logger(__name__)


class LeaderboardFetcher:
    def __init__(self, config: Config, rabbitmq_client: RabbitMQClient):
        self.config = config
        self.rabbitmq_client = rabbitmq_client

    async def start(self) -> None:
        try:
            async with APIClient(self.config) as api_client:
                raw_data = await api_client.fetch_leaderboard()

                if raw_data is None:
                    logger.error("Failed to fetch leaderboard data - API returned None")
                    raise Exception("Failed to fetch leaderboard data")

                processed_players = DataProcessor.process_leaderboard(raw_data)
                success = await self.rabbitmq_client.publish_leaderboard(
                    processed_players
                )

                if not success:
                    logger.error("Failed to publish leaderboard")

        except Exception as e:
            logger.error("Error scraping leaderboard", error=str(e))
