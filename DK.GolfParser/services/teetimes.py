import structlog

from services.api_client import APIClient
from services.data_processors import DataProcessor
from services.config import Config
from services.rabbitmq_client import RabbitMQClient

logger = structlog.get_logger(__name__)


class TeetimesFetcher:
    def __init__(self, config: Config, rabbitmq_client: RabbitMQClient):
        self.config = config
        self.rabbitmq_client = rabbitmq_client

    async def start(self) -> None:
        try:
            async with APIClient(self.config) as api_client:
                raw_data = await api_client.fetch_teetimes()

                if raw_data is None:
                    logger.error("Failed to fetch teetimes data - API returned None")
                    raise Exception("Failed to fetch teetimes data")

                processed_players = DataProcessor.process_teetimes(raw_data)

                for player in processed_players:
                    success = await self.rabbitmq_client.publish_teetimes(player)
                    if not success:
                        logger.error(
                            "Failed to publish tee times",
                            player_id=player["player_id"],
                        )

        except Exception as e:
            logger.error("Error scraping tee times", error=str(e))
