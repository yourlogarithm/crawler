from pydantic_settings import BaseSettings


class CrawlerSettings(BaseSettings):
    mongo_uri: str = 'mongodb://localhost:27017'
    kafka_uri: str = 'localhost:9092'
    tikv_uri: str = '127.0.0.1:2379'
    redis_uri: str = 'redis://localhost:6379'
    log_level: str = 'INFO'
