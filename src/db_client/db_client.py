import psycopg2
from psycopg2.extras import execute_values
from src.config import AppConfig

from src.db_client.db_exception import (
    DBError
)

class DBClient:
    def __init__(self, config: AppConfig):
        self.config = config
        self.connection = None
        self.cursor = None

    def __enter__(self):
        try:
            self.connection = psycopg2.connect(
                database=self.config.db_name,
                user=self.config.db_username,
                password=self.config.db_password,
                host=self.config.db_hostname,
                port="5432"
            )
            self.cursor = self.connection.cursor()
            return self
        
        except psycopg2.Error as e:
            raise DBError(f"Connection failed: {str(e)}") from e

    def __exit__(self, exc_type, exc, tb):
        if self.cursor:
            self.cursor.close()
        
        if self.connection:
            self.connection.close()
        return False
    
    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()
        
    def _insert(self, query: str, data: tuple) -> None:
        if not self.connection or self.connection.closed:
            raise DBError("No active connection")
        
        try:
            self.cursor.execute(query, data)

        except psycopg2.Error as e:
            self.rollback()
            print(f"Error message: {e.diag.message_primary}")
            print(f"SQL state: {e.diag.sqlstate}")
            print(f"Error position: {e.diag.statement_position}")
            raise DBError(f"Insert failed: {str(e)}") from e
        
    def _upsert(self, query: str, data: list) -> None:
        if not self.connection or self.connection.closed:
            raise DBError("No active connection")
        
        try:
            execute_values(self.cursor, query, data)

        except psycopg2.Error as e:
            self.rollback()
            raise DBError(f"Upsert failed: {str(e)}") from e


    def _update(self, query: str, data: tuple) -> None:
        if not self.connection or self.connection.closed:
            raise DBError("No active connection")
        
        try:
            self.cursor.execute(query, data)
        
        except psycopg2.Error as e:
            self.rollback()
            raise DBError(f"Update failed: {str(e)}") from e

    def _select(self, query: str, data: tuple = None) -> list[tuple]:
        if not self.connection or self.connection.closed:
            raise DBError("No active connection")
        
        try:
            self.cursor.execute(query, data)
            rows = self.cursor.fetchall()
            return rows

        except psycopg2.Error as e:
            raise DBError(f"Select failed: {str(e)}") from e
