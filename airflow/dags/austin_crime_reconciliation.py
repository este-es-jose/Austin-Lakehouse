import pendulum

from airflow.sdk import dag, task


@dag(
    dag_id="austin_crime_reconciliation",
    description=(
        "Compare the Austin crime source with Bronze."
    ),
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="America/Chicago",
    ),
    catchup=False,
    max_active_runs=1,
    tags=["austin-crime", "reconciliation"],
)
def austin_crime_reconciliation():
    @task(retries=0)
    def create_source_snapshot():
        from src.reconciliation.reconcile_source import (
            run_source_reconciliation,
        )

        return run_source_reconciliation()

    @task(retries=1)
    def upload_snapshot(snapshot_result):
        from src.storage.upload_reconciliation import (
            upload_reconciliation_snapshot,
        )

        upload_result = (
            upload_reconciliation_snapshot(
                snapshot_result["run_id"]
            )
        )

        return {
            **snapshot_result,
            **upload_result,
        }

    @task(retries=0)
    def reconcile_with_bronze(upload_result):
        import subprocess

        run_id = upload_result["run_id"]

        if not run_id:
            raise ValueError(
                "No reconciliation run ID was provided."
            )

        packages = (
            "org.apache.hadoop:"
            "hadoop-aws:3.3.4,"
            "org.apache.iceberg:"
            "iceberg-spark-runtime-3.5_2.12:1.11.0,"
            "org.apache.iceberg:"
            "iceberg-aws-bundle:1.11.0,"
            "org.postgresql:"
            "postgresql:42.7.10"
        )

        subprocess.run(
            [
                "spark-submit",
                "--driver-memory",
                "6g",
                "--conf",
                "spark.jars.ivy=/tmp/.ivy2",
                "--packages",
                packages,
                (
                    "/opt/airflow/project/src/"
                    "processing/reconcile_bronze.py"
                ),
                run_id,
            ],
            check=True,
        )

        return upload_result

    snapshot_result = create_source_snapshot()
    upload_result = upload_snapshot(snapshot_result)
    reconcile_with_bronze(upload_result)


austin_crime_reconciliation()
