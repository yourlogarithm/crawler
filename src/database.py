import asyncio
from datetime import datetime
from hashlib import sha512

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument
from pyngleton import singleton

from logger import Logger


@singleton
class DBProvider:
    def __init__(self, mongo_uri: str):
        self._mongo_db = AsyncIOMotorClient(mongo_uri).search_engine

    @staticmethod
    def _shutdown_handler(_signum, _frame):
        asyncio.run(DBProvider().close())

    async def update(self, url: str, title: str, content: str, timestamp: datetime):
        encoded = content.encode('utf-8')
        page_hash = sha512(encoded).digest()
        before = await self._mongo_db.pages.find_one_and_update(
            {'url': url},
            {'$set': {'hash': page_hash, 'title': title, 'timestamp': timestamp}},
            upsert=True,
            return_document=ReturnDocument.BEFORE,
            projection={'hash': True}
        )
        if before is not None and before['hash'] != page_hash:
            count = await self._mongo_db.edges.count_documents({'hash': before['hash']})
            if count == 0:
                Logger().debug(f'Deleting {before["hash"].hex()} from TiKV and MongoDB')
                await self._tikv_client.delete(before['hash'])
