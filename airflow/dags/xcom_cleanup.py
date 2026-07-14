import logging

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "Anh",
    "depends_on_past": False,
}

# XComs are ephemeral inter-task plumbing, not audit data — the durable audit
# trail is the pipeline_run table in the project DB. 30 days keeps recent runs
# inspectable in the UI while stopping unbounded growth from the daily
# market_data_pipeline (whose aggregate/validation payloads are large).
_RETENTION_DAYS = 30

# Anchored to the logical date (not wall clock) so reruns of a past interval
# delete up to the same boundary — idempotent.
_CLEAN_BEFORE = "{{ macros.ds_add(ds, -%d) }}T00:00:00+00:00" % _RETENTION_DAYS


@dag(
    "xcom_cleanup",
    schedule="0 1 * * 0",  # 1 AM UTC Sundays — after universe_maintenance (00:00)
    default_args=default_args,
    catchup=False,
    tags=["financial_data_lake", "maintenance", "airflow"],
    description="Weekly purge of XCom rows older than %d days from the Airflow metadata DB" % _RETENTION_DAYS,
)
def xcom_cleanup():

    # Airflow 3 task code cannot reach the metadata DB through the ORM (Task SDK
    # isolation), so cleanup goes through the sanctioned `airflow db clean` CLI.
    # Scope is deliberately xcom-only: task_instance / dag_run / log history is
    # kept for the UI and debugging. --skip-archive drops rows outright instead
    # of copying them into _airflow_deleted__* tables (which would keep the
    # space allocated and defeat the purpose).
    clean_xcom = BashOperator(
        task_id="clean_xcom",
        bash_command=(
            "airflow db clean "
            "--tables xcom "
            f"--clean-before-timestamp '{_CLEAN_BEFORE}' "
            "--skip-archive "
            "--yes"
        ),
    )

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> clean_xcom >> end


xcom_cleanup()
