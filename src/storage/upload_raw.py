import json
import os
import sys
from pathlib import Path
from uuid import UUID

import boto3
import psycopg
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")

BUCKET_NAME = "austin-crime-lakehouse"


def main():
    if len(sys.argv) != 2:
        raise ValueError(
            "Usage: python upload_raw.py <run_id>"
        )

    run_id = sys.argv[1]

    run_directory = (
        PROJECT_ROOT
        / "data"
        / "raw"
        / "crime_reports"
        / run_id
    )

    manifest_file = (
        run_directory / "manifest.json"
    )

    if not manifest_file.exists():
        raise FileNotFoundError(
            f"Manifest not found: {manifest_file}"
        )

    with manifest_file.open(
        "r",
        encoding="utf-8",
    ) as file:
        manifest = json.load(file)

    extract_batch_id = UUID(
        manifest["extract_batch_id"]
    )

    file_metadata = {
        item["file_name"]: item
        for item in manifest["files"]
    }

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

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv(
        "MINIO_ENDPOINT_URL",
        "http://localhost:9000",
        ),
        aws_access_key_id=os.getenv(
            "MINIO_ROOT_USER"
        ),
        aws_secret_access_key=os.getenv(
            "MINIO_ROOT_PASSWORD"
        ),
        region_name="us-east-1",
    )

    def upload_file(local_file):
        object_key = (
            f"raw/crime_reports/"
            f"{run_directory.name}/"
            f"{local_file.name}"
        )

        s3.upload_file(
            str(local_file),
            BUCKET_NAME,
            object_key,
        )

        print(f"Uploaded: {object_key}")

        return object_key


    def register_raw_file(
        connection,
        local_file,
        object_key,
        metadata,
    ):
        query = """
            insert into ops_control.raw_file_manifest (
                extract_batch_id,
                file_name,
                object_path,
                record_count,
                file_size_bytes
            )
            values (%s, %s, %s, %s, %s)

            on conflict (extract_batch_id, object_path)
            do update set
                record_count = excluded.record_count,
                file_size_bytes = excluded.file_size_bytes
        """

        object_path = (
            f"s3://{BUCKET_NAME}/{object_key}"
        )

        values = (
            extract_batch_id,
            local_file.name,
            object_path,
            metadata["record_count"],
            metadata["file_size_bytes"],
        )

        connection.execute(query, values)


    part_files = sorted(
        run_directory.glob("part_*.json")
    )

    with psycopg.connect(
        **postgres_config,
        autocommit=True,
    ) as connection:

        for local_file in part_files:
            metadata = file_metadata[
                local_file.name
            ]

            actual_size = (
                local_file.stat().st_size
            )
            expected_size = metadata[
                "file_size_bytes"
            ]

            if actual_size != expected_size:
                raise ValueError(
                    "File size mismatch: "
                    f"{local_file.name}"
                )

            object_key = upload_file(local_file)

            register_raw_file(
                connection,
                local_file,
                object_key,
                metadata,
            )

    upload_file(manifest_file)

    print(
        f"Uploaded {len(part_files)} parts "
        f"and the manifest"
    )


if __name__ == "__main__":
    main()
