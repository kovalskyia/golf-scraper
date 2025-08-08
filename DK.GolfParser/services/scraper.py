import asyncio
import structlog

from services.config import Config
from services.entrylist import EntrylistFetcher
from services.teetimes import TeetimesFetcher
from services.leaderboard import LeaderboardFetcher
from services.shots import ShotsFetcher
from services.rabbitmq_client import RabbitMQClient

logger = structlog.get_logger(__name__)


class Scraper:
    def __init__(self, config: Config):
        self.running = False
        self.config = config
        self._scrape_task = None
        self.rabbitmq_client = RabbitMQClient(config)
        self.entrylist_fetcher = EntrylistFetcher(config, self.rabbitmq_client)
        self.teetimes_fetcher = TeetimesFetcher(config, self.rabbitmq_client)
        self.leaderboard_fetcher = LeaderboardFetcher(config, self.rabbitmq_client)
        self.shots_fetcher = ShotsFetcher(config, self.rabbitmq_client)

    async def start(self) -> None:
        try:
            logger.info("Starting Scraper service")

            # Connect to RabbitMQ
            await self.rabbitmq_client.connect()

            # Start scraping loop
            self.running = True
            self._scrape_task = asyncio.create_task(self._scrape_loop())
            # await self._scrape_loop()

            logger.info("Scraper service started successfully")

        except Exception as e:
            logger.error("Failed to start scraper service", error=str(e))
            raise

    async def shutdown(self) -> None:
        logger.info("Shutting down Scraper service")
        self.running = False
        if self._scrape_task:
            self._scrape_task.cancel()
        if self.rabbitmq_client:
            await self.rabbitmq_client.disconnect()
        logger.info("Scraper service shutdown complete")

    async def _scrape_loop(self) -> None:
        while self.running:
            try:
                logger.info("Starting scraping cycle")

                # Fetch and process all data types
                await self._scrape_entrylist()
                await self._scrape_teetimes()
                await self._scrape_leaderboard()
                await self._scrape_shots()

                logger.info("Completed scraping cycle")

                # Wait for next cycle
                await asyncio.sleep(self.config.scrape_interval)

            except Exception as e:
                logger.error("Error in scraping loop", error=str(e))
                await asyncio.sleep(60)  # Wait before retrying

    async def _scrape_entrylist(self) -> None:
        await self.entrylist_fetcher.start()

    async def _scrape_teetimes(self) -> None:
        await self.teetimes_fetcher.start()

    async def _scrape_leaderboard(self) -> None:
        await self.leaderboard_fetcher.start()

    async def _scrape_shots(self) -> None:
        await self.shots_fetcher.start()
