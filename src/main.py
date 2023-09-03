import json
from datetime import datetime
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup
from bs4.element import SoupStrainer
from fastapi import FastAPI
from redis import asyncio as aioredis

from constants import USER_AGENT, ENCODINGS, RANKER_TOPIC
from database import DBProvider
from messaging import Producer
from politeness import PolitenessChecker
from settings import CrawlerSettings
from common_utils import persistent_execution
from common_utils.logger import Logger

app = FastAPI()
app_settings = CrawlerSettings()
http_client = aiohttp.ClientSession(headers={'User-Agent': USER_AGENT})
producer = Producer(app_settings.kafka_uri)
logger = Logger(app_settings.log_level)


@app.on_event('startup')
async def startup_event():
    await persistent_execution(producer.inner.start, tries=5, delay=5, backoff=5, logger_=logger)
    DBProvider(app_settings.mongo_uri)
    redis_client = await aioredis.from_url(app_settings.redis_uri)
    PolitenessChecker(http_client, redis_client)
    logger.info('Crawler started')


@app.on_event('shutdown')
async def shutdown_event():
    await producer.inner.stop()
    await PolitenessChecker().close()
    await http_client.close()
    logger.info('Crawler stopped')


@app.post('/crawl')
async def crawl(url: str):
    logger.info(f'{url} | Crawling')
    if not await PolitenessChecker().should_crawl(url, logger):
        logger.debug(f'{url} | Cannot crawl')
        return
    
    logger.debug(f'{url} Fetching')
    async with http_client.get(url) as response:
        content_type = response.headers.get('Content-Type', '')
        if 'charset=' in content_type:
            charset = content_type.split('charset=')[-1].split(';')[0].strip()
            logger.debug(f'{url} | Getting text using {charset} encoding')
            text = await response.text(encoding=charset)
        else:
            for encoding in ENCODINGS:
                try:
                    text = await response.text(encoding=encoding)
                    logger.debug(f'{url} | Getting text using {encoding} encoding')
                    break
                except UnicodeDecodeError:
                    continue
            else:
                return

    logger.debug(f'{url} | Parsing')
    soup = BeautifulSoup(text, 'html.parser', parse_only=SoupStrainer(('a', 'title')))
    title_tag = soup.find('title')
    title = title_tag.string if title_tag is not None else None
    now = datetime.now()
    links = [link.get('href') for link in soup.find_all('a')]
    links = set(urljoin(url, link).split('#')[0] for link in links if link is not None)
    logger.debug(f'{url} | Updating and sending to queue')
    links_list = list(links)
    await DBProvider().update(url, title, text, now, logger)
    timestamp_ms = int(now.timestamp() * 1000)
    await producer.send_to_queue(RANKER_TOPIC, json.dumps([url, links_list]).encode('utf-8'), timestamp_ms)
    await producer.send_urls_batch(links_list, timestamp_ms)
    logger.info(f'{url} | Done')
