from src.services.universe_service import UniverseService
from src.services.ohlcv_service import OHLCVService
from src.services.delta_maintenance_service import DeltaMaintenanceService
from src.clients.schwab_client import SchwabClient
from src.clients.schwab_token import (
    RedisTokenStore, RedisLock, RedisErrorMarker, SchwabTokenProvider,
)
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
    # The token provider BORROWS this same DBClient (read-only) for its seed
    # read, so it lives inside the service's connection lifecycle.
    schwab = build_schwab_client(config, client)
    s3 = S3DeltaClient(config)
    return OHLCVService(client, schwab, s3, dag_id, pipeline_run_id)

def get_delta_maintenance_service(retention_hours: int = 168) -> DeltaMaintenanceService:
    # No DBClient: maintenance is storage-only, outside any Postgres transaction.
    config = AppConfig()
    s3 = S3DeltaClient(config)
    return DeltaMaintenanceService(s3, retention_hours=retention_hours)

def build_schwab_client(config, db: DBClient) -> SchwabClient:
    r = redis.Redis(
        host=config.redis_host,
        port=config.redis_port,
        decode_responses=False
    )

    store = RedisTokenStore(r, key="schwab:access_token")
    lock = RedisLock(r, lock_key="schwab:token_refresh_lock", ttl_seconds=30)
    error_marker = RedisErrorMarker(r, key="schwab:refresh_error", ttl_seconds=45)

    provider = SchwabTokenProvider(
        store=store,
        lock=lock,
        error_marker=error_marker,
        db=db,
        client_id=config.schwab_client_id,
        client_secret=config.schwab_client_secret,
        base_url=config.schwab_base_url,
    )

    return SchwabClient(
        config=config,
        token_provider=provider
    )