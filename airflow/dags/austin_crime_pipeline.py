import pendulum

from airflow.sdk import TriggerRule, dag, task


@dag(
    dag_id="austin_crime_pipeline",
    description="Orchestrate the Austin crime lakehouse.",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="America/Chicago",
    ),
    catchup=False,
    max_active_runs=1,
    tags=["austin-crime"],
)
def austin_crime_pipeline():
    @task(retries=0)
    def extract_incremental():
        from src.ingestion.extract_incremental import (
            run_incremental_extraction,
        )

        return run_incremental_extraction()

    @task.branch
    def choose_path(extraction_result):
        if extraction_result["has_changes"]:
            return "upload_raw"

        return "no_changes"

    @task
    def no_changes(extraction_result):
        print("No source changes were found.")
        print(
            "Pipeline run: "
            f"{extraction_result['pipeline_run_id']}"
        )

    @task(retries=1)
    def upload_raw(extraction_result):
        import subprocess
        import sys

        run_id = extraction_result["run_id"]

        if not run_id:
            raise ValueError(
                "No extraction run ID was provided."
            )

        subprocess.run(
            [
                sys.executable,
                (
                    "/opt/airflow/project/"
                    "src/storage/upload_raw.py"
                ),
                run_id,
            ],
            check=True,
        )

        return extraction_result

    @task(retries=0)
    def refresh_bronze(extraction_result):
        import subprocess

        run_id = extraction_result["run_id"]
        load_mode = extraction_result["load_mode"]

        if not run_id:
            raise ValueError(
                "No extraction run ID was provided."
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

        command = [
            "spark-submit",
            "--driver-memory",
            "6g",
            "--conf",
            "spark.jars.ivy=/tmp/.ivy2",
            "--packages",
            packages,
            (
                "/opt/airflow/project/src/"
                "processing/raw_to_bronze.py"
            ),
            run_id,
        ]

        if load_mode == "FULL_SNAPSHOT":
            command.append("--full-snapshot")

        elif load_mode != "INCREMENTAL":
            raise ValueError(
                "Unsupported Bronze load mode: "
                f"{load_mode}"
            )

        subprocess.run(
            command,
            check=True,
        )

        return extraction_result

    @task(retries=0)
    def refresh_silver(extraction_result):
        import subprocess

        packages = (
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
                    "processing/bronze_to_silver.py"
                ),
            ],
            check=True,
        )

        return extraction_result

    @task(retries=0)
    def refresh_gold(extraction_result):
        import subprocess

        packages = (
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
                    "processing/silver_to_gold.py"
                ),
            ],
            check=True,
        )

        return extraction_result

    @task(retries=0)
    def run_dbt_build(extraction_result):
        import subprocess

        subprocess.run(
            [
                "dbt",
                "build",
                "--project-dir",
                "/opt/airflow/project/austin_crime",
                "--profiles-dir",
                "/opt/airflow/dbt",
            ],
            check=True,
        )

        return extraction_result

    @task(retries=0)
    def validate_pipeline(extraction_result):
        import subprocess
        import sys

        subprocess.run(
            [
                sys.executable,
                (
                    "/opt/airflow/project/src/"
                    "validation/validate_pipeline.py"
                ),
            ],
            check=True,
        )

        return extraction_result

    @task(retries=0)
    def finalize_success(extraction_result):
        from src.operations.finalize_pipeline import (
            finalize_pipeline_success,
        )

        result = finalize_pipeline_success(
            pipeline_run_id=extraction_result[
                "pipeline_run_id"
            ],
            window_end=extraction_result[
                "window_end"
            ],
        )

        print(result)

        return result

    @task(
        retries=0,
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    def finalize_failure(extraction_result):
        from src.operations.finalize_pipeline import (
            finalize_pipeline_failure,
        )

        result = finalize_pipeline_failure(
            pipeline_run_id=extraction_result[
                "pipeline_run_id"
            ],
            failure_category=(
                "AIRFLOW_TASK_FAILURE"
            ),
            error_message=(
                "A processing task failed. "
                "Review the Airflow task logs."
            ),
        )

        print(result)

        return result

    extraction_result = extract_incremental()

    selected_path = choose_path(
        extraction_result
    )

    no_changes_task = no_changes(
        extraction_result
    )

    upload_result = upload_raw(
        extraction_result
    )

    bronze_result = refresh_bronze(
        upload_result
    )

    silver_result = refresh_silver(
        bronze_result
    )

    gold_result = refresh_gold(
        silver_result
    )

    dbt_result = run_dbt_build(
        gold_result
    )

    validation_result = validate_pipeline(
        dbt_result
    )

    finalization_result = finalize_success(
        validation_result
    )

    [
        upload_result,
        bronze_result,
        silver_result,
        gold_result,
        dbt_result,
        validation_result,
        finalization_result,
    ] >> finalize_failure(
        extraction_result
    )

    selected_path >> [
        no_changes_task,
        upload_result,
    ]


austin_crime_pipeline()
