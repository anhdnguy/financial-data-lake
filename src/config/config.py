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
        self.schwab_client_id: str = os.getenv("SCHWAB_CLIENT_ID")
        self.schwab_client_secret: str = os.getenv("SCHWAB_CLIENT_SECRET")
        self.schwab_base_url: str = os.getenv("SCHWAB_BASE_URL", "https://api.schwabapi.com")

        # Redis — optional; only required for market_data_pipeline token management
        self.redis_host: str = os.getenv("REDIS_HOST", "localhost")
        self.redis_port: int = int(os.getenv("REDIS_PORT", "6379"))

        # S3 / Delta Lake — optional; only required for market_data_pipeline write layer
        self.s3_endpoint_url: str = os.getenv("S3_ENDPOINT_URL", "http://localstack:4566")
        self.aws_access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID", "test")
        self.aws_secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
        self.aws_region: str = os.getenv("AWS_REGION", "us-east-1")
        self.delta_table_uri: str = os.getenv("DELTA_TABLE_URI", "s3://ohlcv/prices")