from datetime import datetime, timezone

from airflow.sdk import dag, task


@dag(
    dag_id="pipeline_smoke_test",
    description="Confirm that local Airflow can run a task.",
    schedule=None,
    start_date=datetime(
        2026,
        1,
        1,
        tzinfo=timezone.utc,
    ),
    catchup=False,
    tags=["austin-crime", "test"],
)
def pipeline_smoke_test():
    @task
    def confirm_airflow():
        print("Airflow orchestration is working.")

    confirm_airflow()


pipeline_smoke_test()
