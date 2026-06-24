import logging
from typing import Dict, List

from src.clients.s3_client import S3DeltaClient
from src.clients.s3_exception import S3Error
from src.services.pipeline_exception import PipelineStorageError

logger = logging.getLogger(__name__)


class DeltaMaintenanceService:
    """
    Owns Delta Lake table maintenance: OPTIMIZE (compaction) + VACUUM (tombstone cleanup).

    Unlike OHLCVService / UniverseService, this service holds NO DBClient and has NO
    __enter__/__exit__. Maintenance is a pure storage concern — it touches only the
    Delta-on-S3 boundary and never PostgreSQL, so there is no transaction lifecycle to
    own. (Pipeline-run logging is intentionally skipped for this DAG.)

    retention_hours is the VACUUM safety window (time travel / in-flight readers), not a
    business data-retention policy — see S3DeltaClient.vacuum.
    """

    def __init__(self, s3: S3DeltaClient, retention_hours: int = 168):
        self.s3 = s3
        self.retention_hours = retention_hours

    def optimize_table(self) -> Dict:
        try:
            metrics = self.s3.optimize()
        except S3Error as e:
            logger.error("optimize_table failed: %s", e)
            raise PipelineStorageError("Delta optimize failed") from e
        logger.info("OPTIMIZE metrics: %s", metrics)
        return metrics

    def vacuum_table(self, dry_run: bool = False) -> List[str]:
        try:
            removed = self.s3.vacuum(retention_hours=self.retention_hours, dry_run=dry_run)
        except S3Error as e:
            logger.error("vacuum_table failed: %s", e)
            raise PipelineStorageError("Delta vacuum failed") from e
        logger.info(
            "VACUUM removed %d tombstoned files (retention_hours=%d, dry_run=%s)",
            len(removed), self.retention_hours, dry_run,
        )
        return removed
