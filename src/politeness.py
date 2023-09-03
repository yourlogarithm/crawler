import asyncio
import time
from collections import defaultdict
from logging import Logger
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import aiohttp
from pyngleton import singleton
from redis import asyncio as aioredis

from constants import REDIS_EXPIRE, USER_AGENT


@singleton
class PolitenessChecker:
    def __init__(self, session: aiohttp.ClientSession, redis_client: aioredis.Redis):
        self._session = session
        self._redis = redis_client
        self._rp = RobotFileParser()
        self._borrow_lock = asyncio.Lock()
        self._locks = defaultdict(asyncio.Lock)
        self._usage_count = defaultdict(int)

    @staticmethod
    def get_robots_txt_url(url: str):
        parsed_url = urlparse(url)
        robots_txt_url = f'{parsed_url.scheme}://{parsed_url.netloc}/robots.txt'
        return robots_txt_url

    async def _borrow(self, url: str):
        async with self._borrow_lock:
            self._usage_count[url] += 1
            return self._locks[url]

    async def _return(self, url: str):
        async with self._borrow_lock:
            self._usage_count[url] -= 1
            if self._usage_count[url] == 0:
                del self._locks[url]
                del self._usage_count[url]

    async def can_crawl(self, url: str, logger: Logger) -> bool:
        logger.debug(f'{url} | Checking politeness')
        robots_txt_url = self.get_robots_txt_url(url)

        lock = await self._borrow(robots_txt_url)
        async with lock:
            if (can_fetch := await self._redis.get(robots_txt_url)) is not None:
                logger.debug(f'{url} | Found robots.txt in redis by domain {robots_txt_url} - {can_fetch == b"1"}')
                return can_fetch == b'1'
            else:
                logger.debug(f'{url} | Fetching robots.txt')
                async with self._session.get(url) as response:
                    try:
                        text = await response.text()
                    except UnicodeDecodeError:
                        text = ''
        await self._return(robots_txt_url)

        self._rp.parse(text.splitlines())
        can_fetch = self._rp.can_fetch(USER_AGENT, url)

        await self._redis.set(robots_txt_url, 1 if can_fetch else 0, exat=int(time.time()) + REDIS_EXPIRE)

        return can_fetch

    async def should_crawl(self, url: str, logger: Logger):
        try:
            async with self._session.head(url) as response:
                content_type = response.headers.get('Content-Type', '')
            content_type_passed = 'text/html' in content_type
            if not content_type_passed:
                logger.debug(f'{url} | Cannot crawl due to content type')
                return False
            robots_passed = await self.can_crawl(url, logger)
            if not robots_passed:
                logger.debug(f'{url} | Cannot crawl due to robots.txt')
                return False
            return True
        except Exception as e:
            logger.error(f"{url} | {e}")
            return False

    async def close(self):
        await self._redis.close()
