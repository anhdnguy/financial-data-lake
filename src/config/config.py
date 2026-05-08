import os
import logging

logger = logging.getLogger(__name__)

class AppConfig:
    def __init__(self):
        """
        Application config loaded from environment variables

        DB_username: Database username
        DB_password: Database password
        DB_hostname: Database hostname
        DB_name: Database name
        """

        self.db_username: str = os.getenv("DB_USER")
        self.db_password: str = os.getenv("DB_PASSWORD")
        self.db_hostname: str = os.getenv("DB_HOSTNAME", "localhost")
        self.db_name: str = os.getenv("DB_DB")

        if not self.db_username:
            raise ValueError("Required environment variable DB_USER is not set")

        if not self.db_password:
            raise ValueError("Required environment variable DB_PASSWORD is not set")

        if not self.db_name:
            raise ValueError("Required environment variable DB_DB is not set")