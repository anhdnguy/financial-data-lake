from src.services.universe_service import UniverseService

# Import config
from src.config import AppConfig

# Import client
from src.db_client.db_client import DBClient

def get_service(dag_id: str, pipeline_run_id: str) -> UniverseService:
    config = AppConfig()
    client = DBClient(config)
    return UniverseService(client, dag_id, pipeline_run_id)