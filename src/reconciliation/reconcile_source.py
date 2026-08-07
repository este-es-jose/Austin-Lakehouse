import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from trino.dbapi import connect

DATASET_ID = "fdj4-gpfu"

API_URL = (
    "https://data.austintexas.gov/"
    f"api/v3/views/{DATASET_ID}/query.json"
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RECONCILIATION_ROOT = (
    PROJECT_ROOT
    / "data"
    / "reconciliation"
    / "crime_reports"
)

PAGE_SIZE = 10000

# Keep this at 2 while testing.
MAX_PAGES = None

load_dotenv(PROJECT_ROOT / ".env")

app_token = os.getenv("SOCRATA_APP_TOKEN")

if not app_token:
    raise ValueError(
        "SOCRATA_APP_TOKEN was not found."
    )

headers = {
    "X-App-Token": app_token,
}


def get_source_summary():
    query = (
        "SELECT "
        "count(*) AS record_count, "
        "max(`:updated_at`) AS max_updated_at"
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

    return (
        int(result["record_count"]),
        result["max_updated_at"],
    )


def get_bronze_summary():
    connection = connect(
        host=os.getenv(
            "TRINO_HOST",
            "localhost",
        ),
        port=8080,
        user="reconciliation",
        catalog="lakehouse",
        schema="bronze",
    )

    query = """
        SELECT
            count(*) AS record_count,
            max(source_updated_at) AS max_updated_at
        FROM crime_reports
    """

    try:
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
    finally:
        connection.close()

    return result


def fetch_source_key_page(
    window_end,
    page_number,
):
    query = (
        "SELECT incident_report_number, "
        "`:id`, `:updated_at` "
        f"WHERE `:updated_at` <= '{window_end}' "
        "ORDER BY incident_report_number"
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
        },
        timeout=60,
    )

    response.raise_for_status()

    return response.json()


def save_source_key_page(
    records,
    page_number,
    output_directory,
):
    file_path = (
        output_directory
        / f"part_{page_number:04d}.json"
    )

    with file_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(records, file)

    return file_path


def download_source_keys(
    window_end,
    expected_record_count,
):
    started_at = datetime.now(timezone.utc)

    run_id = started_at.strftime(
        "%Y%m%dT%H%M%SZ"
    )

    output_directory = (
        RECONCILIATION_ROOT / run_id
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=False,
    )

    page_number = 1
    pages_downloaded = 0
    records_downloaded = 0
    files = []

    while True:
        records = fetch_source_key_page(
            window_end,
            page_number,
        )

        if not records:
            break

        file_path = save_source_key_page(
            records,
            page_number,
            output_directory,
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
        records_downloaded += len(records)

        print(
            f"Page {page_number}: "
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

    completed_at = datetime.now(timezone.utc)

    if records_downloaded == expected_record_count:
        status = "SUCCESS"
    elif (
        MAX_PAGES is not None
        and pages_downloaded >= MAX_PAGES
    ):
        status = "PARTIAL"
    else:
        status = "COUNT_MISMATCH"

    manifest = {
        "dataset_id": DATASET_ID,
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "window_end": window_end,
        "page_size": PAGE_SIZE,
        "max_pages": MAX_PAGES,
        "expected_record_count": (
            expected_record_count
        ),
        "pages_downloaded": pages_downloaded,
        "records_downloaded": records_downloaded,
        "status": status,
        "files": files,
    }

    manifest_path = (
        output_directory / "manifest.json"
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

    return (
        output_directory,
        manifest_path,
        manifest,
    )


def compare_summaries(
    source_count,
    source_max_updated,
    bronze_count,
    bronze_max_updated,
):
    summaries_match = True

    if source_count != bronze_count:
        summaries_match = False

        print(
            "Warning: Source and Bronze counts do not "
            "match: "
            f"source={source_count}, "
            f"bronze={bronze_count}"
        )

    if (
        source_max_updated is None
        or bronze_max_updated is None
    ):
        timestamps_match = (
            source_max_updated == bronze_max_updated
        )

    else:
        source_timestamp = datetime.fromisoformat(
            source_max_updated.replace(
                "Z",
                "+00:00",
            )
        )

        bronze_timestamp = datetime.fromisoformat(
            bronze_max_updated.replace(
                "Z",
                "+00:00",
            )
        )

        timestamps_match = (
            source_timestamp == bronze_timestamp
        )

    if not timestamps_match:
        summaries_match = False

        print(
            "Warning: Source and Bronze maximum "
            "update timestamps do not match."
        )

    if summaries_match:
        print("Source and Bronze summaries match.")

    else:
        print(
            "Summary differences will be investigated "
            "by the full reconciliation."
        )

    return summaries_match


def run_source_reconciliation():
    source_count, source_max_updated = (
        get_source_summary()
    )

    bronze_count, bronze_max_updated = (
        get_bronze_summary()
    )

    print(f"Source record count: {source_count}")
    print(
        "Source maximum updated at: "
        f"{source_max_updated}"
    )

    print(f"Bronze record count: {bronze_count}")
    print(
        "Bronze maximum updated at: "
        f"{bronze_max_updated}"
    )

    summaries_match = compare_summaries(
        source_count,
        source_max_updated,
        bronze_count,
        bronze_max_updated,
    )

    (
        output_directory,
        manifest_path,
        manifest,
    ) = download_source_keys(
        source_max_updated,
        source_count,
    )

    print(f"Status: {manifest['status']}")
    print(
        "Pages downloaded: "
        f"{manifest['pages_downloaded']}"
    )
    print(
        "Records downloaded: "
        f"{manifest['records_downloaded']}"
    )
    print(f"Snapshot directory: {output_directory}")
    print(f"Manifest: {manifest_path}")

    if manifest["status"] != "SUCCESS":
        raise ValueError(
            "The reconciliation snapshot did not "
            "complete successfully: "
            f"{manifest['status']}"
        )

    return {
        "run_id": manifest["run_id"],
        "status": manifest["status"],
        "record_count": manifest[
            "records_downloaded"
        ],
        "window_end": manifest["window_end"],
        "summaries_match": summaries_match,
    }


def main():
    result = run_source_reconciliation()

    print("Reconciliation result:")
    print(
        json.dumps(
            result,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
