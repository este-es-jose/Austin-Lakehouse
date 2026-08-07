import json
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import requests
from dotenv import load_dotenv


DATASET_ID = "fdj4-gpfu"

API_URL = (
    f"https://data.austintexas.gov/"
    f"api/v3/views/{DATASET_ID}/query.json"
)

PAGE_SIZE = 10000

# Keep this at 1 for the controlled test.
# Change it to None for a complete extraction.
MAX_PARTS = 1

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_ROOT = PROJECT_ROOT / "data" / "raw" / "crime_reports"


load_dotenv(PROJECT_ROOT / ".env")

app_token = os.getenv("SOCRATA_APP_TOKEN")

if not app_token:
    raise ValueError("SOCRATA_APP_TOKEN was not found in .env")

postgres_config = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

if not all(postgres_config.values()):
    raise ValueError("PostgreSQL settings were not found in .env")

headers = {
    "X-App-Token": app_token,
}


def get_source_count():
    response = requests.post(
        API_URL,
        headers=headers,
        json={
            "query": "SELECT count(*) AS record_count",
        },
        timeout=60,
    )

    response.raise_for_status()

    return int(response.json()[0]["record_count"])


def fetch_batch(last_incident_number=None):
    where_clause = ""

    if last_incident_number is not None:
        where_clause = (
            f"WHERE `incident_report_number` "
            f"> {int(last_incident_number)}"
        )

    query = (
        f"SELECT * "
        f"{where_clause} "
        f"ORDER BY `incident_report_number` "
        f"LIMIT {PAGE_SIZE}"
    )

    response = requests.post(
        API_URL,
        headers=headers,
        json={
            "query": query,
            "includeSystem": True,
            "includeSynthetic": True,
        },
        timeout=60,
    )

    response.raise_for_status()

    return response.json()


def save_batch(records, part_number, raw_directory):
    file_path = raw_directory / f"part_{part_number:04d}.json"

    with file_path.open("w", encoding="utf-8") as file:
        json.dump(
            records,
            file,
            ensure_ascii=False,
        )
    return file_path


def create_pipeline_run(started_at):
    query = """
        insert into ops_control.pipeline_run (
            pipeline_name,
            run_type,
            status,
            started_at
        )
        values (%s, %s, %s, %s)
        returning pipeline_run_id
    """

    values = (
        "crime_reports_pipeline",
        "FULL",
        "RUNNING",
        started_at,
    )

    with psycopg.connect(**postgres_config) as connection:
        result = connection.execute(query, values)
        pipeline_run_id = result.fetchone()[0]

    return pipeline_run_id


def finish_pipeline_run(
    pipeline_run_id,
    status,
    failure_category=None,
    error_message=None,
):
    query = """
        update ops_control.pipeline_run
        set
            status = %s,
            finished_at = current_timestamp,
            failure_category = %s,
            error_message = %s
        where pipeline_run_id = %s
    """

    values = (
        status,
        failure_category,
        error_message,
        pipeline_run_id,
    )

    with psycopg.connect(**postgres_config) as connection:
        connection.execute(query, values)

def create_extract_batch(
    pipeline_run_id,
    started_at,
    raw_directory,
):
    query = """
        insert into ops_control.extract_batch (
            pipeline_run_id,
            dataset_id,
            source_url,
            status,
            page_size,
            raw_prefix,
            started_at
        )
        values (%s, %s, %s, %s, %s, %s, %s)
        returning extract_batch_id
    """

    values = (
        pipeline_run_id,
        DATASET_ID,
        API_URL,
        "RUNNING",
        PAGE_SIZE,
        str(raw_directory),
        started_at,
    )

    with psycopg.connect(**postgres_config) as connection:
        result = connection.execute(query, values)
        extract_batch_id = result.fetchone()[0]

    return extract_batch_id


def finish_extract_batch(
    extract_batch_id,
    status,
    pages_downloaded,
    record_count,
    manifest_path=None,
):
    query = """
        update ops_control.extract_batch
        set
            status = %s,
            pages_downloaded = %s,
            record_count = %s,
            manifest_path = %s,
            finished_at = current_timestamp
        where extract_batch_id = %s
    """

    if manifest_path is not None:
        manifest_path = str(manifest_path)

    values = (
        status,
        pages_downloaded,
        record_count,
        manifest_path,
        extract_batch_id,
    )

    with psycopg.connect(**postgres_config) as connection:
        connection.execute(query, values)

def main():
    started_at = datetime.now(timezone.utc)
    pipeline_run_id = create_pipeline_run(started_at)

    extract_batch_id = None
    extracted_record_count = 0
    parts_downloaded = 0
    manifest_path = None

    try:
        run_id = started_at.strftime("%Y%m%dT%H%M%SZ")

        raw_directory = RAW_ROOT / run_id
        raw_directory.mkdir(parents=True)

        extract_batch_id = create_extract_batch(
            pipeline_run_id,
            started_at,
            raw_directory,
        )

        expected_record_count = get_source_count()

        part_number = 1
        last_incident_number = None
        first_incident_number = None

        files = []

        while True:
            records = fetch_batch(
                last_incident_number
            )

            if not records:
                break

            file_path = save_batch(
                records,
                part_number,
                raw_directory,
            )

            files.append(
                {
                    "file_name": file_path.name,
                    "record_count": len(records),
                    "file_size_bytes": (
                        file_path.stat().st_size
                    ),
                }
            )

            parts_downloaded += 1

            if first_incident_number is None:
                first_incident_number = (
                    records[0][
                        "incident_report_number"
                    ]
                )

            last_incident_number = (
                records[-1][
                    "incident_report_number"
                ]
            )

            extracted_record_count += len(records)

            print(
                f"Part {part_number}: "
                f"{len(records)} records"
            )

            if len(records) < PAGE_SIZE:
                break

            if (
                MAX_PARTS is not None
                and part_number >= MAX_PARTS
            ):
                break

            part_number += 1

        if (
            MAX_PARTS is not None
            and extracted_record_count
            < expected_record_count
        ):
            extraction_status = "PARTIAL"

        elif (
            extracted_record_count
            == expected_record_count
        ):
            extraction_status = "SUCCESS"

        else:
            extraction_status = "COUNT_MISMATCH"

        completed_at = datetime.now(timezone.utc)

        manifest = {
            "pipeline_run_id": str(
                pipeline_run_id
            ),
            "run_id": run_id,
            "dataset_id": DATASET_ID,
            "source_url": API_URL,
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "page_size": PAGE_SIZE,
            "expected_record_count": (
                expected_record_count
            ),
            "extracted_record_count": (
                extracted_record_count
            ),
            "parts_downloaded": parts_downloaded,
            "first_incident_report_number": (
                first_incident_number
            ),
            "last_incident_report_number": (
                last_incident_number
            ),
            "status": extraction_status,
            "extract_batch_id": str(
                extract_batch_id
            ),
            "files": files,
        }

        manifest_path = (
            raw_directory / "manifest.json"
        )

        with manifest_path.open(
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                manifest,
                file,
                indent=2,
            )

        finish_extract_batch(
            extract_batch_id,
            extraction_status,
            parts_downloaded,
            extracted_record_count,
            manifest_path,
        )

        if extraction_status == "SUCCESS":
            finish_pipeline_run(
                pipeline_run_id,
                "SUCCESS",
            )

        else:
            finish_pipeline_run(
                pipeline_run_id,
                "FAILED",
                "DATA_QUALITY_FAILURE",
                (
                    "Extraction finished with status: "
                    f"{extraction_status}"
                ),
            )

        print(f"Status: {extraction_status}")
        print(
            f"Records downloaded: "
            f"{extracted_record_count}"
        )
        print(f"Raw directory: {raw_directory}")

    except Exception as error:
        if extract_batch_id is not None:
            finish_extract_batch(
                extract_batch_id,
                "FAILED",
                parts_downloaded,
                extracted_record_count,
                manifest_path,
            )

        finish_pipeline_run(
            pipeline_run_id,
            "FAILED",
            "UNKNOWN_ERROR",
            str(error),
        )

        raise


if __name__ == "__main__":
    main()
