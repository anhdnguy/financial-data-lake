import csv

from src.db_client.db_client import DBClient

from src.transform.universe_transform import _get_today, _convert_list_to_dict

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

        with self.client:
            self.client._insert(query, data)

    def pull_symbol(self):
        csv_dir = "../../stock_csv/tickers.csv"
        with open(csv_dir, 'r') as file:
            reader = csv.DictReader(file)
            list_tickers = [row for row in reader]
        
        dict_tickers = _convert_list_to_dict(list_tickers)
        return dict_tickers

    def sanitize_symbol(self):
        pass