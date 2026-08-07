import json
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import requests
from dotenv import load_dotenv


DATASET_ID = "fdj4-gpfu"

PAGE_SIZE = 10000

BULK_REVIEW_THRESHOLD = 0.80

# Use None for a complete extraction.
# Set an integer for a controlled page limit.
MAX_PAGES = None

API_URL = (
    f"https://data.austintexas.gov/"
    f"api/v3/views/{DATASET_ID}/query.json"
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_ROOT = PROJECT_ROOT / "data" / "raw" / "crime_reports"

load_dotenv(PROJECT_ROOT / ".env")

app_token = os.getenv("SOCRATA_APP_TOKEN")

if not app_token:
    raise ValueError(
        "SOCRATA_APP_TOKEN was not found in .env"
    )

postgres_config = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

if not all(postgres_config.values()):
    raise ValueError(
        "PostgreSQL settings were not found in .env"
    )

headers = {
    "X-App-Token": app_token,
}


def get_watermark():
    query = """
        select watermark_value
        from ops_control.source_watermark
        where dataset_id = %s
    """

    with psycopg.connect(**postgres_config) as connection:
        result = connection.execute(
            query,
            (DATASET_ID,),
        )

        row = result.fetchone()

    if row is None or row[0] is None:
        raise ValueError(
            f"No watermark found for {DATASET_ID}"
        )

    return row[0]


def get_change_window(watermark):
    watermark_utc = watermark.astimezone(
        timezone.utc
    )

    watermark_text = (
        watermark_utc
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )

    query = (
        "SELECT "
        "count(*) AS record_count, "
        "max(`:updated_at`) AS max_updated_at "
        f"WHERE `:updated_at` > '{watermark_text}'"
    )

    response = requests.post(
        API_URL,
        headers=headers,
        json={
            "query": query,
            "includeSystem": True,
        },
        timeout=60,
    )

    response.raise_for_status()

    result = response.json()[0]

    record_count = int(result["record_count"])
    window_end = result.get("max_updated_at")

    return record_count, window_end


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


def choose_load_mode(
    changed_record_count,
    source_record_count,
):
    if changed_record_count < 0:
        raise ValueError(
            "Changed record count cannot be negative."
        )

    if source_record_count < 0:
        raise ValueError(
            "Source record count cannot be negative."
        )

    if changed_record_count > source_record_count:
        raise ValueError(
            "Changed record count cannot exceed "
            "the source record count."
        )

    if changed_record_count == 0:
        return "NO_CHANGES"

    if changed_record_count == source_record_count:
        return "FULL_SNAPSHOT"

    change_ratio = (
        changed_record_count / source_record_count
    )

    if change_ratio >= BULK_REVIEW_THRESHOLD:
        return "REVIEW_REQUIRED"

    return "INCREMENTAL"


def fetch_changed_page(
    watermark,
    window_end,
    page_number,
):
    watermark_utc = watermark.astimezone(
        timezone.utc
    )

    watermark_text = (
        watermark_utc
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )

    query = (
        "SELECT * "
        f"WHERE `:updated_at` > '{watermark_text}' "
        f"AND `:updated_at` <= '{window_end}' "
        "ORDER BY `:updated_at`, `:id`"
    )

    response = requests.post(
        API_URL,
        headers=headers,
        json={
            "query": query,
            "page": {
                "pageNumber": page_number,
                "pageSize": PAGE_SIZE,
            },
            "includeSystem": True,
            "includeSynthetic": True,
        },
        timeout=60,
    )

    response.raise_for_status()

    return response.json()


def save_changed_page(
    records,
    part_number,
    raw_directory,
):
    file_path = (
        raw_directory
        / f"part_{part_number:04d}.json"
    )

    with file_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            records,
            file,
            ensure_ascii=False,
        )

    return file_path


def create_changed_run(
    started_at,
    raw_directory,
    load_mode,
):
    pipeline_query = """
        insert into ops_control.pipeline_run (
            pipeline_name,
            run_type,
            status,
            started_at
        )
        values (%s, %s, %s, %s)
        returning pipeline_run_id
    """

    batch_query = """
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

    with psycopg.connect(**postgres_config) as connection:
        pipeline_result = connection.execute(
            pipeline_query,
            (
                "crime_reports_incremental",
                load_mode,
                "RUNNING",
                started_at,
            ),
        )

        pipeline_run_id = (
            pipeline_result.fetchone()[0]
        )

        batch_result = connection.execute(
            batch_query,
            (
                pipeline_run_id,
                DATASET_ID,
                API_URL,
                "RUNNING",
                PAGE_SIZE,
                str(raw_directory),
                started_at,
            ),
        )

        extract_batch_id = (
            batch_result.fetchone()[0]
        )

    return pipeline_run_id, extract_batch_id


def finish_changed_batch(
    extract_batch_id,
    status,
    pages_downloaded,
    record_count,
    manifest_path,
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

    manifest_value = None

    if manifest_path is not None:
        manifest_value = str(manifest_path)

    values = (
        status,
        pages_downloaded,
        record_count,
        manifest_value,
        extract_batch_id,
    )

    with psycopg.connect(**postgres_config) as connection:
        connection.execute(query, values)


def fail_changed_run(
    pipeline_run_id,
    failure_category,
    error_message,
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
        "FAILED",
        failure_category,
        error_message,
        pipeline_run_id,
    )

    with psycopg.connect(**postgres_config) as connection:
        connection.execute(query, values)


def record_non_extracting_run(
    run_type,
    pipeline_status,
    batch_status,
    failure_category=None,
    error_message=None,
):
    pipeline_query = """
        insert into ops_control.pipeline_run (
            pipeline_name,
            run_type,
            status,
            started_at,
            finished_at,
            failure_category,
            error_message
        )
        values (
            %s,
            %s,
            %s,
            current_timestamp,
            current_timestamp,
            %s,
            %s
        )
        returning pipeline_run_id
    """

    batch_query = """
        insert into ops_control.extract_batch (
            pipeline_run_id,
            dataset_id,
            source_url,
            status,
            page_size,
            pages_downloaded,
            record_count,
            started_at,
            finished_at
        )
        values (
            %s,
            %s,
            %s,
            %s,
            %s,
            0,
            0,
            current_timestamp,
            current_timestamp
        )
        returning extract_batch_id
    """

    with psycopg.connect(**postgres_config) as connection:
        pipeline_result = connection.execute(
            pipeline_query,
            (
                "crime_reports_incremental",
                run_type,
                pipeline_status,
                failure_category,
                error_message,
            ),
        )

        pipeline_run_id = (
            pipeline_result.fetchone()[0]
        )

        batch_result = connection.execute(
            batch_query,
            (
                pipeline_run_id,
                DATASET_ID,
                API_URL,
                batch_status,
                PAGE_SIZE,
            ),
        )

        extract_batch_id = (
            batch_result.fetchone()[0]
        )

    return pipeline_run_id, extract_batch_id


def record_no_change_run():
    return record_non_extracting_run(
        run_type="NO_CHANGES",
        pipeline_status="SUCCESS",
        batch_status="SUCCESS",
    )


def record_review_required_run(error_message):
    return record_non_extracting_run(
        run_type="REVIEW_REQUIRED",
        pipeline_status="FAILED",
        batch_status="BLOCKED",
        failure_category="BULK_CHANGE_REVIEW",
        error_message=error_message,
    )


def extract_changed_records(
    watermark,
    window_end,
    expected_record_count,
    source_record_count,
    load_mode,
):
    started_at = datetime.now(timezone.utc)
    run_id = started_at.strftime("%Y%m%dT%H%M%SZ")

    raw_directory = RAW_ROOT / run_id
    raw_directory.mkdir(parents=True)

    pipeline_run_id = None
    extract_batch_id = None
    pages_downloaded = 0
    extracted_record_count = 0
    manifest_path = None

    try:
        pipeline_run_id, extract_batch_id = (
            create_changed_run(
                started_at,
                raw_directory,
                load_mode,
            )
        )

        page_number = 1
        files = []

        while True:
            records = fetch_changed_page(
                watermark,
                window_end,
                page_number,
            )

            if not records:
                break

            file_path = save_changed_page(
                records,
                page_number,
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

            pages_downloaded += 1
            extracted_record_count += len(records)

            print(
                f"Part {page_number}: "
                f"{len(records)} records"
            )

            if len(records) < PAGE_SIZE:
                break

            if (
                MAX_PAGES is not None
                and page_number >= MAX_PAGES
            ):
                break

            page_number += 1

        if (
            extracted_record_count
            == expected_record_count
        ):
            extraction_status = "SUCCESS"

        elif (
            MAX_PAGES is not None
            and pages_downloaded >= MAX_PAGES
        ):
            extraction_status = "PARTIAL"

        else:
            extraction_status = "COUNT_MISMATCH"

        completed_at = datetime.now(timezone.utc)

        manifest = {
            "pipeline_run_id": str(
                pipeline_run_id
            ),
            "extract_batch_id": str(
                extract_batch_id
            ),
            "run_id": run_id,
            "run_type": load_mode,
            "load_mode": load_mode,
            "dataset_id": DATASET_ID,
            "source_url": API_URL,
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "watermark_start": (
                watermark.isoformat()
            ),
            "window_end": window_end,
            "page_size": PAGE_SIZE,
            "expected_record_count": (
                expected_record_count
            ),
            "source_record_count": (
                source_record_count
            ),
            "extracted_record_count": (
                extracted_record_count
            ),
            "pages_downloaded": pages_downloaded,
            "status": extraction_status,
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

        finish_changed_batch(
            extract_batch_id,
            extraction_status,
            pages_downloaded,
            extracted_record_count,
            manifest_path,
        )

        if extraction_status != "SUCCESS":
            fail_changed_run(
                pipeline_run_id,
                "DATA_QUALITY_FAILURE",
                (
                    "Extraction finished "
                    f"with status: {extraction_status}"
                ),
            )

        print(f"Status: {extraction_status}")
        print(
            f"Records downloaded: "
            f"{extracted_record_count}"
        )
        print(f"Raw directory: {raw_directory}")
        print(f"Manifest: {manifest_path}")

        return {
            "has_changes": True,
            "pipeline_run_id": str(
                pipeline_run_id
            ),
            "extract_batch_id": str(
                extract_batch_id
            ),
            "run_id": run_id,
            "record_count": (
                extracted_record_count
            ),
            "window_end": window_end,
            "load_mode": load_mode,
            "status": extraction_status,
        }

    except Exception as error:
        if extract_batch_id is not None:
            finish_changed_batch(
                extract_batch_id,
                "FAILED",
                pages_downloaded,
                extracted_record_count,
                manifest_path,
            )

        if pipeline_run_id is not None:
            fail_changed_run(
                pipeline_run_id,
                "UNKNOWN_ERROR",
                str(error),
            )

        raise


def run_incremental_extraction():
    watermark = get_watermark()

    changed_record_count, window_end = (
        get_change_window(watermark)
    )

    source_record_count = get_source_count()

    load_mode = choose_load_mode(
        changed_record_count,
        source_record_count,
    )

    if source_record_count == 0:
        change_percentage = 0
    else:
        change_percentage = (
            changed_record_count
            / source_record_count
            * 100
        )

    print(f"Current watermark: {watermark}")
    print(f"Source records: {source_record_count}")
    print(f"Changed records: {changed_record_count}")
    print(
        "Changed percentage: "
        f"{change_percentage:.2f}%"
    )
    print(f"Change window ends at: {window_end}")
    print(f"Load mode: {load_mode}")

    if load_mode == "REVIEW_REQUIRED":
        review_message = (
            "The changed record percentage is at or "
            "above the bulk-review threshold, but the "
            "batch is not a complete source snapshot. "
            "Review the source before downloading. "
            f"changed={changed_record_count}, "
            f"source={source_record_count}, "
            f"percentage={change_percentage:.2f}%"
        )

        pipeline_run_id, extract_batch_id = (
            record_review_required_run(
                review_message
            )
        )

        print(
            "Review-required pipeline run recorded: "
            f"{pipeline_run_id}"
        )
        print(
            "Blocked extraction batch recorded: "
            f"{extract_batch_id}"
        )

        raise RuntimeError(review_message)

    if changed_record_count == 0:
        pipeline_run_id, extract_batch_id = (
            record_no_change_run()
        )

        print("No incremental extraction needed.")
        print(
            f"Pipeline run recorded: "
            f"{pipeline_run_id}"
        )
        print(
            f"Extraction batch recorded: "
            f"{extract_batch_id}"
        )

        return {
            "has_changes": False,
            "pipeline_run_id": str(pipeline_run_id),
            "extract_batch_id": str(extract_batch_id),
            "run_id": None,
            "record_count": 0,
            "window_end": None,
            "load_mode": load_mode,
            "status": "SUCCESS",
        }

    result = extract_changed_records(
        watermark,
        window_end,
        changed_record_count,
        source_record_count,
        load_mode,
    )

    if result["status"] != "SUCCESS":
        raise RuntimeError(
            "Incremental extraction did not complete "
            f"successfully: {result['status']}"
        )

    return result


def main():
    result = run_incremental_extraction()

    print("Extraction result:")
    print(
        json.dumps(
            result,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
