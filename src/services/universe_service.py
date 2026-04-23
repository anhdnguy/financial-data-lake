import pandas as pd

from src.db_client.db_client import DBClient

from src.transform.universe_transform import _get_today

class UniverseService:
    def __init__(self, client: DBClient, dag_id: str, pipeline_run_id: str):
        self.client = client
        self.pipeline_run_id = pipeline_run_id
        self.dag_id = dag_id

    def pipeline_start(self):
        query = """
            INSERT INTO pipeline_run (
                id, dag_id, run_date, status
            ) VALUES %s
        """

        _today = _get_today()

        data = (self.pipeline_run_id, self.dag_id, _today, "RUNNING")

        self.client.log_pipeline_run(query, data)