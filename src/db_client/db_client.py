import psycopg2
from src.config import AppConfig

from src.db_client.db_execption import (
    DBError
)

class DBClient:
    def __init__(self, config: AppConfig):
        self.config = config
        self.connection = None
        self.cursor = None

    def _connect_to_db(self) -> None:
        try:
            self.connection = psycopg2.connect(
                database=self.config.db_name,
                user=self.config.db_username,
                password=self.config.db_password,
                host=self.config.db_hostname,
                port="5432"
            )
            self.cursor = self.connection.cursor()
        
        except psycopg2.Error as e:
            raise DBError("Error") from e
        
    def _close_connection(self) -> None:
        if self.cursor:
            self.cursor.close()
        
        if self.connection:
            self.connection.close()
        
    def _insert(self, query: str, data: tuple) -> None:
        try:
            self.cursor.execute(f"{query} VALUES{data}")
            self.connection.commit()

        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Error message: {e.diag.message_primary}")
            print(f"SQL state: {e.diag.sqlstate}")
            print(f"Error position: {e.diag.statement_position}")
            raise DBError("Error") from e


    def _update(self):
        pass

    def _select(self):
        pass


    def log_pipeline_run(self, query: str, data: tuple):
        self._connect_to_db()
        self._insert(query, data)
        self._close_connection()
