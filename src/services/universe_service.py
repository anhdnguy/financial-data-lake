import csv
from typing import List, Dict

from src.db_client.db_client import DBClient

from src.transform.universe_transform import (
    _get_today, _convert_list_to_dict, _sanitize_symbol
)

class UniverseService:
    def __init__(self, client: DBClient, dag_id: str, pipeline_run_id: str):
        self.client = client
        self.pipeline_run_id = pipeline_run_id
        self.dag_id = dag_id

    def __enter__(self):
        self.client._connect_to_db()
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None:
            self.client.connection.rollback()
        else:
            self.client.connection.commit()
        self.client._close_connection()
        return False

    def pipeline_start(self):
        query = """
            INSERT INTO pipeline_run (
                id, dag_id, run_date, status
            ) VALUES (%s, %s, %s, %s)
        """

        _today = _get_today()

        data = (self.pipeline_run_id, self.dag_id, _today, "RUNNING")

        self.client._insert(query, data)

    def pull_symbol(self):
        csv_dir = "../../stock_csv/tickers.csv"
        with open(csv_dir, 'r') as file:
            reader = csv.DictReader(file)
            list_tickers = [row for row in reader]
        
        dict_tickers = _convert_list_to_dict(list_tickers)
        return dict_tickers

    def sanitize_symbol(self, symbol_lists: Dict[str, List]):
        """
        Ex: {<universe_id>: ["ticker1", "ticker2"]}
        """
        return _sanitize_symbol(symbol_lists)