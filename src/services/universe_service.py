import csv
import logging
from pathlib import Path
from datetime import date
from typing import List, Dict, Optional

from src.clients.db_client import DBClient
from src.clients.db_exception import DBError
from src.services.pipeline_exception import PipelineIOError, PipelineDBError

from src.transform.universe_transform import (
    _get_today, _convert_list_to_dict, _sanitize_symbol,
    _convert_tuple_to_list, _sort_delisted_from_active,
    _add_constants_to_tuple, _list_to_tuple
)

logger = logging.getLogger(__name__)

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
        try:
            with open(csv_dir, 'r') as file:
                reader = csv.DictReader(file)
                list_tickers = [row for row in reader]
        except FileNotFoundError:
            logger.error("Tickers CSV not found at %s", csv_dir)
            raise PipelineIOError(f"Tickers CSV not found: {csv_dir}")
        except csv.Error as e:
            logger.error("Failed to parse tickers CSV: %s", e)
            raise PipelineIOError(f"Tickers CSV parse error: {e}") from e

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

    def update_exit_date(self, current_delisted_symbols: Dict[str, Dict[str, List]]):
        query_select_membership = """
            SELECT id FROM membership WHERE symbol = ANY(%s)
        """
        query_update_exit_date = """
            UPDATE universe_membership
            SET exit_date = %s
            WHERE membership_id = ANY(%s::uuid[]) AND universe_id = %s AND exit_date IS NULL
        """
        _today = _get_today()
        try:
            for universe in current_delisted_symbols:
                delisted_list = current_delisted_symbols[universe]["delisted_symbols"]
                if not delisted_list:
                    continue
                rows = self.client._select(query_select_membership, (delisted_list,))
                membership_ids = [row[0] for row in rows]
                if membership_ids:
                    self.client._update(query_update_exit_date, (_today, membership_ids, universe))
        except DBError as e:
            logger.error("update_exit_date failed: %s", e)
            raise PipelineDBError("Exit date update failed") from e
        return current_delisted_symbols

    def pipeline_end(self):
        query = """
            UPDATE pipeline_run SET status = %s, completed_at = NOW() WHERE id = %s
        """
        try:
            self.client._update(query, ('SUCCESS', self.pipeline_run_id))
        except DBError as e:
            logger.error("pipeline_end failed: %s", e)
            raise PipelineDBError("Pipeline end update failed") from e

    def upsert_membership_universe(self, current_delisted_symbols: Dict[str, Dict[str, List]]):
        query_upsert_membership = """
            INSERT INTO membership (symbol)
            VALUES %s ON CONFLICT (symbol) DO NOTHING
        """
        query_select_membership = """
            SELECT id FROM membership WHERE symbol = ANY(%s)
        """
        query_upsert_universe_membership = """
            INSERT INTO universe_membership (membership_id, universe_id, enter_date)
            VALUES %s ON CONFLICT (membership_id, universe_id) WHERE exit_date IS NULL DO NOTHING
        """
        _today = _get_today()
        try:
            for universe in current_delisted_symbols:
                current_list = current_delisted_symbols[universe]["new_symbols"]
                self.client._upsert(query_upsert_membership, _list_to_tuple(current_list))

                current_list_membership = _add_constants_to_tuple(
                    self.client._select(query_select_membership, (current_list,)), (universe, _today)
                )
                self.client._upsert(query_upsert_universe_membership, current_list_membership)
        except DBError as e:
            logger.error("upsert_membership_universe failed for universe %s: %s", universe, e)
            raise PipelineDBError(f"Membership upsert failed for universe {universe}") from e

    def pipeline_failed(
        self,
        pipeline_run_id: Optional[str],
        run_date: date,
        error: str,
    ) -> None:
        try:
            if pipeline_run_id is None:
                rows = self.client._select(
                    "SELECT id FROM pipeline_run WHERE dag_id = %s AND run_date = %s",
                    (self.dag_id, run_date),
                )
                if not rows:
                    logger.error(
                        "pipeline_failed: no run found for dag_id=%s run_date=%s",
                        self.dag_id,
                        run_date,
                    )
                    return
                pipeline_run_id = rows[0][0]

            self.client._update(
                """
                UPDATE pipeline_run
                SET status = 'FAILED', completed_at = NOW(), notes = %s
                WHERE id = %s
                """,
                (error, pipeline_run_id),
            )
        except DBError as e:
            logger.error("pipeline_failed update failed: %s", e)
            raise PipelineDBError("Pipeline failed update failed") from e
