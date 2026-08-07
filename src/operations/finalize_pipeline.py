import os
from datetime import datetime
from pathlib import Path
from uuid import UUID

import psycopg
from dotenv import load_dotenv


DATASET_ID = "fdj4-gpfu"

PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")

postgres_config = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

if not all(postgres_config.values()):
    raise ValueError(
        "PostgreSQL settings were not found."
    )


def validate_inputs(pipeline_run_id, window_end):
    if not pipeline_run_id:
        raise ValueError(
            "pipeline_run_id is required."
        )

    if not window_end:
        raise ValueError(
            "window_end is required."
        )

    parsed_pipeline_run_id = UUID(
        str(pipeline_run_id)
    )

    parsed_window_end = datetime.fromisoformat(
        window_end.replace("Z", "+00:00")
    )

    if parsed_window_end.tzinfo is None:
        raise ValueError(
            "window_end must include a timezone."
        )

    return (
        parsed_pipeline_run_id,
        parsed_window_end,
    )
def finalize_pipeline_success(
    pipeline_run_id,
    window_end,
):
    (
        pipeline_run_id,
        window_end,
    ) = validate_inputs(
        pipeline_run_id,
        window_end,
    )

    pipeline_query = """
        SELECT status
        FROM ops_control.pipeline_run
        WHERE pipeline_run_id = %s
        FOR UPDATE
    """

    watermark_query = """
        SELECT
            watermark_value,
            last_successful_pipeline_run_id
        FROM ops_control.source_watermark
        WHERE dataset_id = %s
        FOR UPDATE
    """

    update_watermark_query = """
        UPDATE ops_control.source_watermark
        SET
            watermark_value = %s,
            last_successful_pipeline_run_id = %s,
            updated_at = current_timestamp
        WHERE dataset_id = %s
    """

    update_pipeline_query = """
        UPDATE ops_control.pipeline_run
        SET
            status = 'SUCCESS',
            finished_at = current_timestamp,
            failure_category = NULL,
            error_message = NULL
        WHERE pipeline_run_id = %s
          AND status = 'RUNNING'
    """

    with psycopg.connect(
        **postgres_config
    ) as connection:
        with connection.transaction():
            pipeline_row = connection.execute(
                pipeline_query,
                (pipeline_run_id,),
            ).fetchone()

            if pipeline_row is None:
                raise ValueError(
                    "Pipeline run was not found."
                )

            watermark_row = connection.execute(
                watermark_query,
                (DATASET_ID,),
            ).fetchone()

            if watermark_row is None:
                raise ValueError(
                    "Source watermark was not found."
                )

            pipeline_status = pipeline_row[0]
            current_watermark = watermark_row[0]
            last_successful_run = watermark_row[1]

            already_finalized = (
                pipeline_status == "SUCCESS"
                and last_successful_run
                == pipeline_run_id
                and current_watermark is not None
                and current_watermark >= window_end
            )

            if already_finalized:
                print(
                    "Pipeline run was already "
                    "finalized successfully."
                )

                return {
                    "pipeline_run_id": str(
                        pipeline_run_id
                    ),
                    "status": "SUCCESS",
                    "watermark_value": (
                        current_watermark.isoformat()
                    ),
                    "already_finalized": True,
                }

            if pipeline_status != "RUNNING":
                raise ValueError(
                    "Pipeline run cannot be finalized "
                    f"from status {pipeline_status}."
                )

            if (
                current_watermark is not None
                and window_end < current_watermark
            ):
                raise ValueError(
                    "The new watermark is older than "
                    "the current watermark."
                )

            watermark_result = connection.execute(
                update_watermark_query,
                (
                    window_end,
                    pipeline_run_id,
                    DATASET_ID,
                ),
            )

            if watermark_result.rowcount != 1:
                raise ValueError(
                    "The source watermark was not updated."
                )

            pipeline_result = connection.execute(
                update_pipeline_query,
                (pipeline_run_id,),
            )

            if pipeline_result.rowcount != 1:
                raise ValueError(
                    "The pipeline run was not updated."
                )

    print("Pipeline finalized successfully.")

    return {
        "pipeline_run_id": str(pipeline_run_id),
        "status": "SUCCESS",
        "watermark_value": window_end.isoformat(),
        "already_finalized": False,
    }

def finalize_pipeline_failure(
    pipeline_run_id,
    failure_category,
    error_message,
):
    if not pipeline_run_id:
        raise ValueError(
            "pipeline_run_id is required."
        )

    if not failure_category:
        raise ValueError(
            "failure_category is required."
        )

    if not error_message:
        raise ValueError(
            "error_message is required."
        )

    pipeline_run_id = UUID(
        str(pipeline_run_id)
    )

    update_query = """
        UPDATE ops_control.pipeline_run
        SET
            status = 'FAILED',
            finished_at = current_timestamp,
            failure_category = %s,
            error_message = %s
        WHERE pipeline_run_id = %s
          AND status = 'RUNNING'
    """

    status_query = """
        SELECT status
        FROM ops_control.pipeline_run
        WHERE pipeline_run_id = %s
    """

    with psycopg.connect(
        **postgres_config
    ) as connection:
        result = connection.execute(
            update_query,
            (
                failure_category,
                error_message,
                pipeline_run_id,
            ),
        )

        if result.rowcount == 1:
            print("Pipeline marked as failed.")

            return {
                "pipeline_run_id": str(
                    pipeline_run_id
                ),
                "status": "FAILED",
                "already_finalized": False,
            }

        pipeline_row = connection.execute(
            status_query,
            (pipeline_run_id,),
        ).fetchone()

        if pipeline_row is None:
            raise ValueError(
                "Pipeline run was not found."
            )

        if pipeline_row[0] == "FAILED":
            print(
                "Pipeline was already marked as failed."
            )

            return {
                "pipeline_run_id": str(
                    pipeline_run_id
                ),
                "status": "FAILED",
                "already_finalized": True,
            }

        raise ValueError(
            "Pipeline run cannot be marked as failed "
            f"from status {pipeline_row[0]}."
        )