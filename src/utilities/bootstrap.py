from src.services.universe_service import UniverseService
from src.services.ohlcv_service import OHLCVService
from src.clients.schwab_client import SchwabClient

from src.config import AppConfig
from src.clients.db_client import DBClient

def get_service(dag_id: str, pipeline_run_id: str) -> UniverseService:
    config = AppConfig()
    client = DBClient(config)
    return UniverseService(client, dag_id, pipeline_run_id)

def get_market_data_service(dag_id: str, pipeline_run_id: str) -> OHLCVService:
    config = AppConfig()
    client = DBClient(config)
    schwab = SchwabClient(config)
    return OHLCVService(client, schwab, dag_id, pipeline_run_id)