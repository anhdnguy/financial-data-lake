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

        # Schwab API — optional; only required for market_data_pipeline
        self.schwab_app_key: str = os.getenv("SCHWAB_APP_KEY")
        self.schwab_app_secret: str = os.getenv("SCHWAB_APP_SECRET")
        self.schwab_refresh_token: str = os.getenv("SCHWAB_REFRESH_TOKEN")
        self.schwab_base_url: str = os.getenv("SCHWAB_BASE_URL", "https://api.schwabapi.com")

        # Redis — optional; only required for market_data_pipeline token management
        self.redis_host: str = os.getenv("REDIS_HOST", "localhost")
        self.redis_port: int = int(os.getenv("REDIS_PORT", "6379"))