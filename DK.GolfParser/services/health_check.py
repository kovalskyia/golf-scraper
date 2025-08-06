import asyncio
import structlog
import socket

from aiohttp import web

logger = structlog.get_logger(__name__)


class HealthCheckServer:
    def __init__(self, port: int = 8000):
        self.port = port
        self.app = web.Application()
        self.runner = None
        self.site = None

        # Setup routes
        self.app.router.add_get("/health", self.health_handler)
        self.app.router.add_get("/metrics", self.metrics_handler)
        self.app.router.add_get("/", self.root_handler)

    def _find_available_port(self, start_port: int) -> int:
        for port in range(start_port, start_port + 100):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(("0.0.0.0", port))
                    return port
            except OSError:
                continue
        raise RuntimeError(
            f"No available ports found in range {start_port}-{start_port + 100}"
        )

    async def start(self) -> None:
        try:
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()

            # Try to find an available port
            actual_port = self._find_available_port(self.port)

            self.site = web.TCPSite(self.runner, "0.0.0.0", actual_port)
            await self.site.start()

            if actual_port != self.port:
                logger.warning(
                    f"Port {self.port} was in use, using port {actual_port} instead"
                )

            logger.info("Health check server started", port=actual_port)

        except Exception as e:
            logger.error("Failed to start health check server", error=str(e))
            raise

    async def shutdown(self) -> None:
        if self.runner:
            await self.runner.cleanup()
            logger.info("Health check server shutdown complete")

    async def health_handler(self, request: web.Request) -> web.Response:
        health_status = {
            "status": "healthy",
            "service": "golf-parser",
            "timestamp": asyncio.get_event_loop().time(),
        }

        return web.json_response(health_status, status=200)

    async def metrics_handler(self, request: web.Request) -> web.Response:
        metrics_data = {"service": "golf-parser", "metrics": {"status": "operational"}}

        return web.json_response(metrics_data, status=200)

    async def root_handler(self, request: web.Request) -> web.Response:
        service_info = {
            "service": "Golf Parser",
            "version": "1.0.0",
            "description": "Masters Tournament data parser",
            "endpoints": {"health": "/health", "metrics": "/metrics"},
        }

        return web.json_response(service_info, status=200)
