import asyncio
import logging
import os
import signal
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.config import Config
from services.logger import setup_logging
from services.entrilist import EntrylistFetcher
# from datadog_setup import setup_datadog 

logger = logging.getLogger(__name__)

class GolfParserApp:
    """Main application class for the golf parser service"""
    def __init__(self):
        self.config = Config()
        self.running = False
        
    async def start(self):
        """Start the golf parser application"""
        try:
            logger.info("Starting Golf Parser Application")
            
            # Setup logging
            setup_logging(self.config.log_level)
            
            EntrylistFetcher().process()
            
        except Exception as e:
            logger.error(f"Failed to start application: {e}")
            await self.shutdown()
            sys.exit(1)
    
    async def shutdown(self):
        """Gracefully shutdown the application"""
        logger.info("Shutting down Golf Parser Application")
        self.running = False
        
        logger.info("Application shutdown complete")

async def main():
    """Main entry point"""
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
    finally:
        await app.shutdown()

if __name__ == "__main__":
    asyncio.run(main()) 