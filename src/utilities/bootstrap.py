from src.services.universe_service import UniverseService
from src.services.ohlcv_service import OHLCVService
from src.clients.schwab_client import SchwabClient
from src.clients.schwab_token import RedisTokenStore, RedisLock, SchwabTokenProvider
from src.clients.s3_client import S3DeltaClient

import redis

from src.config import AppConfig
from src.clients.db_client import DBClient

def get_service(dag_id: str, pipeline_run_id: str) -> UniverseService:
    config = AppConfig()
    client = DBClient(config)
    return UniverseService(client, dag_id, pipeline_run_id)

def get_market_data_service(dag_id: str, pipeline_run_id: str) -> OHLCVService:
    config = AppConfig()
    client = DBClient(config)
    schwab = build_schwab_client()
    s3 = S3DeltaClient(config)
    return OHLCVService(client, schwab, s3, dag_id, pipeline_run_id)

def build_schwab_client() -> SchwabClient:
    r = redis.Redis(
        host=AppConfig.redis_host,
        port=AppConfig.redis_port,
        decode_responses=False
    )

    store = RedisTokenStore(r, key="schwab:access_token")
    lock = RedisLock(r, lock_key="schwab:token_refresh_lock", ttl_seconds=30)

    provider = SchwabTokenProvider(
        store=store,
        lock=lock,
        client_id=AppConfig.schwab_app_key,
        client_secret=AppConfig.schwab_app_secret,
        base_url=AppConfig.schwab_base_url
    )

    return SchwabClient(
        config=AppConfig,
        token_provider=provider
    )