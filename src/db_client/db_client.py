import psycopg2
from psycopg2.extras import execute_values
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

    def __enter__(self):
        self._connect_to_db()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._close_connection()
        
    def _insert(self, query: str, data: tuple) -> None:
        try:
            self.cursor.execute(f"{query} VALUES{data}")
            self.connection.commit()

        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Error message: {e.diag.message_primary}")
            print(f"SQL state: {e.diag.sqlstate}")
            print(f"Error position: {e.diag.statement_position}")
            raise DBError("Insert failed") from e
        
    def _upsert(self, query: str, data: list) -> None:
        try:
            execute_values(self.cursor, query, data)
            self.connection.commit()

        except psycopg2.Error as e:
            self.connection.rollback()
            raise DBError("Upsert failed") from e


    def _update(self, query: str):
        try:
            self.cursor.execute(query)
            self.connection.commit()
        
        except psycopg2.Error as e:
            self.connection.rollback()
            raise DBError("Update failed") from e

    def _select(self, query: str) -> list[tuple]:
        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            self.connection.commit()
            return rows

        except psycopg2.Error as e:
            raise DBError("Select failed") from e
