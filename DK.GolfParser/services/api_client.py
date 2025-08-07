import asyncio
import aiohttp
import structlog

from typing import Dict, Any, Optional
from services.config import Config
from services.metrics import metrics

logger = structlog.get_logger(__name__)


class APIClient:

    def __init__(self, config: Config):
        self.config = config
        self.session = None
        self.headers = {
            "User-Agent": "Golf-Parser/1.0",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        }

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers=self.headers,
            timeout=aiohttp.ClientTimeout(total=self.config.request_timeout),
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_data(
        self, endpoint: str, retries: int = None
    ) -> Optional[Dict[str, Any]]:
        if retries is None:
            retries = self.config.max_retries

        for attempt in range(retries + 1):
            try:
                start_time = asyncio.get_event_loop().time()

                async with self.session.get(endpoint) as response:
                    if response.status == 200:
                        data = await response.json()
                        duration = asyncio.get_event_loop().time() - start_time
                        metrics.record_fetch_latency(endpoint, duration)

                        logger.info(
                            "Successfully fetched data",
                            endpoint=endpoint,
                            duration=duration,
                        )
                        return data
                    else:
                        logger.warning(
                            "HTTP error fetching data",
                            endpoint=endpoint,
                            status=response.status,
                        )

            except asyncio.TimeoutError:
                logger.warning(
                    "Timeout fetching data", endpoint=endpoint, attempt=attempt + 1
                )
            except aiohttp.ClientError as e:
                logger.warning(
                    "Client error fetching data",
                    endpoint=endpoint,
                    error=str(e),
                    attempt=attempt + 1,
                )
            except Exception as e:
                logger.error(
                    "Unexpected error fetching data",
                    endpoint=endpoint,
                    error=str(e),
                    attempt=attempt + 1,
                )

            if attempt < retries:
                sleep_duration = 2**attempt  # Exponential backoff
                logger.info(
                    "Retrying request",
                    endpoint=endpoint,
                    attempt=attempt + 1,
                    sleep_duration=sleep_duration,
                    total_retries=retries,
                )
                await asyncio.sleep(sleep_duration)

        logger.error("Failed to fetch data after all retries", endpoint=endpoint)
        return None

    async def fetch_endpoint(
        self, config_key: str, **kwargs
    ) -> Optional[Dict[str, Any]]:
        if config_key not in self.config.endpoints:
            logger.error(f"Unknown endpoint config key: {config_key}")
            return None

        endpoint = self.config.endpoints[config_key]
        if kwargs:
            try:
                endpoint = endpoint.format(**kwargs)
            except KeyError as e:
                logger.error(
                    f"Missing required parameter for endpoint {config_key}: {e}"
                )
                return None

        return await self.fetch_data(endpoint)

    async def fetch_entrylist(self) -> Optional[Dict[str, Any]]:
        return await self.fetch_endpoint("entrylist")

    async def fetch_teetimes(self) -> Optional[Dict[str, Any]]:
        return await self.fetch_endpoint("teetimes")

    async def fetch_leaderboard(self) -> Optional[Dict[str, Any]]:
        return await self.fetch_endpoint("leaderboard")

    async def fetch_shots(self, player_id: str) -> Optional[Dict[str, Any]]:
        return await self.fetch_endpoint("shots", player_id=player_id)
