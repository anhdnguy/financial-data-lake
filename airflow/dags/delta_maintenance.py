import logging
from datetime import timedelta
from typing import Dict

from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator

from src.utilities.bootstrap import get_delta_maintenance_service

log = logging.getLogger(__name__)

default_args = {
    "owner": "Anh",
    "depends_on_past": False,
}

# VACUUM safety floor in delta-rs (7 days). Protects in-flight reads + time travel.
# NOT a business data-retention window: the current table version (incl. the 20-day
# volatility lookback) is never touched by vacuum, so this stays at the floor.
_RETENTION_HOURS = 168


@dag(
    "delta_maintenance",
    schedule=timedelta(days=25),
    default_args=default_args,
    catchup=False,
    tags=["financial_data_lake", "maintenance", "delta"],
    description="Periodic Delta Lake OPTIMIZE (compaction) + VACUUM (tombstone cleanup)",
)
def delta_maintenance():

    @task
    def optimize() -> Dict:
        # Compacts the daily small files into large ones. Creates tombstones (the old
        # small files) that this run's vacuum will NOT yet remove — they are younger than
        # the retention window. They get cleaned on a later run once past retention.
        service = get_delta_maintenance_service(retention_hours=_RETENTION_HOURS)
        metrics = service.optimize_table()
        log.info("OPTIMIZE complete: %s", metrics)
        return metrics

    @task
    def vacuum() -> int:
        # Physically deletes tombstoned files older than _RETENTION_HOURS (from PRIOR
        # optimize/merge runs). dry_run=False actually removes; returns the count.
        service = get_delta_maintenance_service(retention_hours=_RETENTION_HOURS)
        removed = service.vacuum_table(dry_run=False)
        log.info("VACUUM removed %d tombstoned files", len(removed))
        return len(removed)

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    optimize_task = optimize()
    vacuum_task = vacuum()

    # OPTIMIZE before VACUUM: compaction creates the tombstones that vacuum later reaps.
    start >> optimize_task >> vacuum_task >> end


delta_maintenance()
