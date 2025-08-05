import asyncio
import json
from aiohttp import web
from typing import Dict, Any

import structlog

logger = structlog.get_logger(__name__)

class HealthCheckServer:
    def __init__(self, port: int = 8000):
        self.port = port
        self.app = web.Application()
        self.runner = None
        self.site = None
        
        # Setup routes
        self.app.router.add_get('/health', self.health_handler)
        self.app.router.add_get('/metrics', self.metrics_handler)
        self.app.router.add_get('/', self.root_handler)
    
    async def start(self) -> None:
        try:
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            
            self.site = web.TCPSite(self.runner, '0.0.0.0', self.port)
            await self.site.start()
            
            logger.info("Health check server started", port=self.port)
            
        except Exception as e:
            logger.error("Failed to start health check server", error=str(e))
            raise
    
    async def shutdown(self) -> None:
        if self.runner:
            await self.runner.cleanup()
            logger.info("Health check server shutdown complete")
    
    async def health_handler(self, request: web.Request) -> web.Response:
        health_status = {
            'status': 'healthy',
            'service': 'golf-scraper',
            'timestamp': asyncio.get_event_loop().time()
        }
        
        return web.json_response(health_status, status=200)
    
    async def metrics_handler(self, request: web.Request) -> web.Response:
        metrics_data = {
            'service': 'golf-scraper',
            'metrics': {
                'status': 'operational'
            }
        }
        
        return web.json_response(metrics_data, status=200)
    
    async def root_handler(self, request: web.Request) -> web.Response:
        service_info = {
            'service': 'Golf Scraper',
            'version': '1.0.0',
            'description': 'Masters Tournament data scraper',
            'endpoints': {
                'health': '/health',
                'metrics': '/metrics'
            }
        }
        
        return web.json_response(service_info, status=200) 