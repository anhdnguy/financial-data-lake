import csv
from pathlib import Path
from typing import List, Dict

from src.db_client.db_client import DBClient

from src.transform.universe_transform import (
    _get_today, _convert_list_to_dict, _sanitize_symbol,
    _convert_tuple_to_list, _sort_delisted_from_active,
    _add_today_to_tuple
)

class UniverseService:
    def __init__(self, client: DBClient, dag_id: str, pipeline_run_id: str):
        self.client = client
        self.pipeline_run_id = pipeline_run_id
        self.dag_id = dag_id

    def __enter__(self):
        self.client.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None:
            self.client.rollback()
        else:
            self.client.commit()
        self.client.__exit__(exc_type, exc, tb)
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
        csv_dir = Path(__file__).parent.parent.parent / "stock_csv" / "tickers.csv"
        with open(csv_dir, 'r') as file:
            reader = csv.DictReader(file)
            list_tickers = [row for row in reader]
        
        dict_tickers = _convert_list_to_dict(list_tickers)
        return dict_tickers
    
    def query_symbols(self, symbol_lists: Dict[str, List]):
        active_symbols = {"import": symbol_lists}
        active_symbols["query"] = {}
        for key in symbol_lists:
            query = """
                SELECT m.symbol FROM universe_membership um
                JOIN membership m ON um.membership_id = m.id
                WHERE um.universe_id = %s AND um.exit_date IS NULL
            """

            temp = self.client._select(query, (key,))
            active_symbols["query"][key] = _convert_tuple_to_list(temp)

        return active_symbols

    def upsert_membership_universe(self, current_delisted_symbols: Dict[str, Dict[str, List]]):
        query_upsert_membership = """
            INSERT INTO membership (symbol)
            VALUES %s ON CONFLICT (symbol) DO UPDATE SET
                symbol = EXCLUDED.symbol
        """
        query_select_membership = """
            SELECT id FROM membership WHERE updated_at = %s
        """
        query_upsert_universe_membership = """
            INSERT INTO univese_membersip (membership_id, universe_id)
            VALUES %s ON CONFLICT (membership_id) DO UPDATE SET
                membership_id = EXCLUDED.membership_id
        """
        _today = _get_today()
        for universe in current_delisted_symbols:
            current_list = current_delisted_symbols[universe]["current_symbols"]
            self._upsert(query_upsert_membership, current_list)

            current_list_membership = _add_today_to_tuple(self._select(query_select_membership), (_today,))
            self._upsert(query_upsert_universe_membership, current_list_membership)
