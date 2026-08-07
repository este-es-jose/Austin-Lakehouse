import json
import os
import sys
from pathlib import Path

import boto3
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")

BUCKET_NAME = "austin-crime-lakehouse"

RECONCILIATION_ROOT = (
    PROJECT_ROOT
    / "data"
    / "reconciliation"
    / "crime_reports"
)


def load_snapshot(run_id):
    run_directory = (
        RECONCILIATION_ROOT / run_id
    )

    manifest_path = (
        run_directory / "manifest.json"
    )

    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Manifest not found: {manifest_path}"
        )

    with manifest_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        manifest = json.load(file)

    if manifest["run_id"] != run_id:
        raise ValueError(
            "Manifest run ID does not match "
            "the requested run ID."
        )

    if manifest["status"] != "SUCCESS":
        raise ValueError(
            "Only successful reconciliation "
            "snapshots can be uploaded."
        )

    part_files = sorted(
        run_directory.glob("part_*.json")
    )

    file_metadata = {
        item["file_name"]: item
        for item in manifest["files"]
    }

    if len(file_metadata) != len(manifest["files"]):
        raise ValueError(
            "Duplicate filenames were found "
            "in the manifest."
        )

    local_file_names = {
        local_file.name
        for local_file in part_files
    }

    if local_file_names != set(file_metadata):
        raise ValueError(
            "Local filenames do not match "
            "the manifest."
        )

    if (
        len(part_files)
        != manifest["pages_downloaded"]
    ):
        raise ValueError(
            "Manifest page count does not match "
            "the number of local files."
        )

    manifest_record_count = sum(
        item["record_count"]
        for item in manifest["files"]
    )

    if (
        manifest_record_count
        != manifest["records_downloaded"]
    ):
        raise ValueError(
            "Manifest record totals do not match."
        )

    for local_file in part_files:
        expected_size = file_metadata[
            local_file.name
        ]["file_size_bytes"]

        if local_file.stat().st_size != expected_size:
            raise ValueError(
                "File size does not match the manifest: "
                f"{local_file.name}"
            )

    return (
        run_directory,
        manifest_path,
        manifest,
        part_files,
    )


def get_s3_client():
    minio_user = os.getenv("MINIO_ROOT_USER")
    minio_password = os.getenv(
        "MINIO_ROOT_PASSWORD"
    )

    if not minio_user or not minio_password:
        raise ValueError(
            "MinIO credentials were not found."
        )

    return boto3.client(
        "s3",
        endpoint_url=os.getenv(
            "MINIO_ENDPOINT_URL",
            "http://localhost:9000",
        ),
        aws_access_key_id=minio_user,
        aws_secret_access_key=minio_password,
        region_name="us-east-1",
    )


def upload_snapshot(
    run_id,
    manifest_path,
    part_files,
):
    s3 = get_s3_client()

    object_prefix = (
        f"reconciliation/crime_reports/{run_id}"
    )

    for local_file in part_files:
        object_key = (
            f"{object_prefix}/{local_file.name}"
        )

        s3.upload_file(
            str(local_file),
            BUCKET_NAME,
            object_key,
        )

        print(f"Uploaded: {object_key}")

    manifest_key = (
        f"{object_prefix}/manifest.json"
    )

    # Upload the manifest last so it marks
    # the snapshot as complete.
    s3.upload_file(
        str(manifest_path),
        BUCKET_NAME,
        manifest_key,
    )

    print(f"Uploaded: {manifest_key}")
    print(
        f"Uploaded {len(part_files)} parts "
        "and the manifest."
    )

    return object_prefix


def upload_reconciliation_snapshot(run_id):
    (
        run_directory,
        manifest_path,
        manifest,
        part_files,
    ) = load_snapshot(run_id)

    print(f"Snapshot directory: {run_directory}")
    print(f"Manifest: {manifest_path}")
    print(f"Files validated: {len(part_files)}")
    print(
        "Records validated: "
        f"{manifest['records_downloaded']}"
    )

    object_prefix = upload_snapshot(
        run_id,
        manifest_path,
        part_files,
    )

    return {
        "run_id": run_id,
        "status": manifest["status"],
        "record_count": manifest[
            "records_downloaded"
        ],
        "object_prefix": object_prefix,
    }


def main():
    if len(sys.argv) != 2:
        raise ValueError(
            "Usage: python upload_reconciliation.py "
            "<run_id>"
        )

    run_id = sys.argv[1]

    result = upload_reconciliation_snapshot(run_id)

    print("Upload result:")
    print(
        json.dumps(
            result,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
