import structlog

from services.api_client import APIClient
from services.data_processors import DataProcessor
from services.config import Config
from services.rabbitmq_client import RabbitMQClient

logger = structlog.get_logger(__name__)


class EntrylistFetcher:
    def __init__(self, config: Config, rabbitmq_client: RabbitMQClient):
        self.config = config
        self.rabbitmq_client = rabbitmq_client

    async def start(self) -> None:
        try:
            async with APIClient(self.config) as api_client:
                raw_data = await api_client.fetch_entrylist()

                if raw_data is None:
                    logger.error("Failed to fetch entrylist data - API returned None")
                    raise Exception("Failed to fetch entrylist data")

                processed_players = DataProcessor.process_entrylist(raw_data)

                for player in processed_players:
                    success = await self.rabbitmq_client.publish_entrylist(player)
                    if not success:
                        logger.error(
                            "Failed to publish entrylist",
                            player_id=player["player_id"],
                        )

        except Exception as e:
            logger.error("Error scraping entrylist", error=str(e))
            raise
