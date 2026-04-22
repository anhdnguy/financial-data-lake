import os
import logging

logger = logging.getLogger(__name__)

class AppConfig:
    """
    Application config loaded from environment variables

    DB_username: Database username
    DB_password: Database password
    DB_hostname: Database hostname
    DB_name: Database name
    """

    db_username: str = os.getenv("DB_USER")
    db_password: str = os.getenv("DB_PASSWORD")
    db_hostname: str = os.getenv("DB_HOSTNAME", "localhost")
    db_name: str = os.getenv("DB_DB")

    