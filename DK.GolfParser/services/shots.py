import structlog

from services.api_client import APIClient
from services.data_processors import DataProcessor
from services.config import Config
from services.rabbitmq_client import RabbitMQClient

logger = structlog.get_logger(__name__)


class ShotsFetcher:
    def __init__(self, config: Config, rabbitmq_client: RabbitMQClient):
        self.config = config
        self.rabbitmq_client = rabbitmq_client

    async def start(self) -> None:
        try:
            # First get the entrylist to get player IDs
            async with APIClient(self.config) as api_client:
                entrylist_data = await api_client.fetch_entrylist()

                if not entrylist_data:
                    logger.warning("No entrylist data available for shots scraping")
                    return

                processed_players = DataProcessor.process_entrylist(entrylist_data)

                # Fetch shots data for each player
                for player in processed_players:
                    player_id = player["player_id"]

                    try:
                        shots_data = await api_client.fetch_shots(player_id)

                        if shots_data:
                            processed_shots = DataProcessor.process_shots(
                                shots_data, player_id
                            )
                            success = await self.rabbitmq_client.publish_shots(
                                processed_shots
                            )

                            if not success:
                                logger.error(
                                    "Failed to publish shots", player_id=player_id
                                )
                        else:
                            logger.warning(
                                "No shots data available", player_id=player_id
                            )

                    except Exception as e:
                        logger.error(
                            "Error processing shots for player",
                            player_id=player_id,
                            error=str(e),
                        )
                        continue

        except Exception as e:
            logger.error("Error scraping shots", error=str(e))
