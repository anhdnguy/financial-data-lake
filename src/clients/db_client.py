import logging
from contextlib import contextmanager
from uuid import uuid4

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from src.config import AppConfig

from src.clients.db_exception import (
    DBError, DBConnectionError, DBQueryError
)

logger = logging.getLogger(__name__)

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
            raise DBConnectionError(f"Connection failed: {str(e)}") from e

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

    @contextmanager
    def savepoint(self, name: str = "sp"):
        """
        Nested rollback point INSIDE the caller's transaction.

        Postgres aborts the whole transaction on any statement error: every later
        statement fails with "current transaction is aborted" until someone rolls
        back. That makes "catch the error and carry on" impossible at statement
        level — which is why these methods used to call connection.rollback()
        themselves, silently discarding work the service had already staged.

        A savepoint gives per-statement isolation without that: on error only the
        work since the savepoint is undone, and the surrounding transaction stays
        usable. The service decides where isolation belongs; the client only
        exposes the primitive. The name is quoted as an identifier, never
        interpolated user input.
        """
        unique = f"{name}_{uuid4().hex[:8]}"
        ident = sql.Identifier(unique)
        self.cursor.execute(sql.SQL("SAVEPOINT {}").format(ident))
        try:
            yield
        except Exception:
            self.cursor.execute(sql.SQL("ROLLBACK TO SAVEPOINT {}").format(ident))
            raise
        else:
            self.cursor.execute(sql.SQL("RELEASE SAVEPOINT {}").format(ident))

    def _insert(self, query: str, data: tuple) -> None:
        if not self.connection or self.connection.closed:
            raise DBError("No active connection")

        try:
            self.cursor.execute(query, data)

        except psycopg2.Error as e:
            # No rollback here — transaction control belongs to the service layer's
            # __exit__. A client-level rollback throws away everything the service
            # has staged, which is not this method's call to make. Callers that want
            # to survive a failed statement wrap it in savepoint().
            logger.error("Insert failed — message: %s | sqlstate: %s | position: %s",
                         e.diag.message_primary, e.diag.sqlstate, e.diag.statement_position)
            raise DBQueryError(f"Insert failed: {str(e)}") from e

    def _upsert(self, query: str, data: list) -> None:
        if not self.connection or self.connection.closed:
            raise DBError("No active connection")

        try:
            execute_values(self.cursor, query, data)

        except psycopg2.Error as e:
            raise DBQueryError(f"Upsert failed: {str(e)}") from e


    def _update(self, query: str, data: tuple) -> None:
        if not self.connection or self.connection.closed:
            raise DBError("No active connection")

        try:
            self.cursor.execute(query, data)

        except psycopg2.Error as e:
            raise DBQueryError(f"Update failed: {str(e)}") from e

    def _select(self, query: str, data: tuple = None) -> list[tuple]:
        if not self.connection or self.connection.closed:
            raise DBError("No active connection")
        
        try:
            self.cursor.execute(query, data)
            rows = self.cursor.fetchall()
            return rows

        except psycopg2.Error as e:
            raise DBQueryError(f"Select failed: {str(e)}") from e
