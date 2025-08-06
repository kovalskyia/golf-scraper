import asyncio
import structlog
import signal
import sys

from services.config import Config
from services.logger import setup_logging
from services.health_check import HealthCheckServer
from services.scraper import Scraper

logger = structlog.get_logger(__name__)


class GolfParserApp:
    def __init__(self):
        self.config = Config()
        self.scraper = None
        self.health_server = None
        self.running = False

    async def start(self):
        try:
            # Setup logging
            setup_logging(self.config.log_level)

            logger.info("Starting Golf Parser Application")

            # Initialize scraper
            self.scraper = Scraper(self.config)

            # Start health check server
            self.health_server = HealthCheckServer(self.config.health_port)
            await self.health_server.start()

            # Start scraper
            self.running = True
            await self.scraper.start()

        except Exception as e:
            logger.error(f"Failed to start application: {e}")
            await self.shutdown()
            sys.exit(1)

    async def shutdown(self):
        logger.info("Shutting down Golf Parser Application")
        self.running = False

        if self.scraper:
            await self.scraper.shutdown()

        if self.health_server:
            await self.health_server.shutdown()

        logger.info("Application shutdown complete")


async def main():
    app = GolfParserApp()

    # Setup signal handlers
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}")
        asyncio.create_task(app.shutdown())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        await app.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Application error: {e}")
    finally:
        await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
