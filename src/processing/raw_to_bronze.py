import os
import sys

from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    to_timestamp,
    trim,
)


postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_db = os.getenv("POSTGRES_DB")

if len(sys.argv) not in (2, 3):
    raise ValueError(
        "Usage: raw_to_bronze.py <run_id> "
        "[--full-snapshot]"
    )

run_id = sys.argv[1]

full_snapshot = False

if len(sys.argv) == 3:
    if sys.argv[2] != "--full-snapshot":
        raise ValueError(
            "The optional argument must be "
            "--full-snapshot."
        )

    full_snapshot = True

ingested_at = datetime.now(
    timezone.utc
).isoformat()

spark = (
    SparkSession.builder
    .appName("Raw to Bronze Crime Reports")
    .master("local[4]")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config(
        "spark.hadoop.fs.s3a.access.key",
        os.getenv("AWS_ACCESS_KEY_ID")
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.lakehouse.type", "jdbc")
    .config(
        "spark.sql.catalog.lakehouse.uri",
        f"jdbc:postgresql://postgres:5432/{postgres_db}"
    )
    .config("spark.sql.catalog.lakehouse.jdbc.user", postgres_user)
    .config("spark.sql.catalog.lakehouse.jdbc.password", postgres_password)
    .config(
        "spark.sql.catalog.lakehouse.jdbc.schema-version",
        "V1"
    )
    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        "s3://austin-crime-lakehouse/warehouse"
    )
    .config(
        "spark.sql.catalog.lakehouse.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )
    .config(
        "spark.sql.catalog.lakehouse.s3.endpoint",
        "http://minio:9000"
    )
    .config(
        "spark.sql.catalog.lakehouse.s3.path-style-access",
        "true"
    )
    .config(
        "spark.sql.catalog.lakehouse.client.region",
        "us-east-1"
    )
    .config(
        "spark.sql.session.timeZone",
        "UTC"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_prefix = (
    "s3a://austin-crime-lakehouse/"
    "raw/crime_reports/"
    f"{run_id}/"
)

raw_file = f"{raw_prefix}part_*.json"

raw_df = (
    spark.read
    .option("multiLine", "true")
    .json(raw_file)
)

bronze_df = (
    raw_df
    .withColumnRenamed(":id", "source_row_id")
    .withColumnRenamed(":version", "source_version")
    .withColumnRenamed(":created_at", "source_created_at")
    .withColumnRenamed(":updated_at", "source_updated_at")
    .withColumn("dataset_id", lit("fdj4-gpfu"))
    .withColumn("ingestion_run_id", lit(run_id))
    .withColumn(
        "ingested_at",
        to_timestamp(lit(ingested_at)),
    )
    .withColumn(
        "source_file",
        lit(raw_prefix),
    )
)

incoming_count = bronze_df.count()

if incoming_count == 0:
    raise ValueError(
        "The incoming Raw batch contains no records."
    )

missing_business_key_count = (
    bronze_df
    .filter(
        col("incident_report_number").isNull()
        | (trim(col("incident_report_number")) == "")
    )
    .count()
)

if missing_business_key_count > 0:
    raise ValueError(
        f"Found {missing_business_key_count} records "
        "without an incident_report_number."
    )

missing_source_id_count = (
    bronze_df
    .filter(
        col("source_row_id").isNull()
        | (trim(col("source_row_id")) == "")
    )
    .count()
)

if missing_source_id_count > 0:
    raise ValueError(
        f"Found {missing_source_id_count} records "
        "without a source_row_id."
    )

missing_updated_at_count = (
    bronze_df
    .filter(col("source_updated_at").isNull())
    .count()
)

if missing_updated_at_count > 0:
    raise ValueError(
        f"Found {missing_updated_at_count} records "
        "without source_updated_at."
    )

dedupe_window = (
    Window
    .partitionBy("incident_report_number")
    .orderBy(
        to_timestamp(
            col("source_updated_at"),
            "yyyy-MM-dd'T'HH:mm:ss.SSSX",
        ).desc_nulls_last(),
        col("source_version")
        .cast("long")
        .desc_nulls_last(),
        col("source_row_id").desc_nulls_last(),
    )
)

bronze_updates_df = (
    bronze_df
    .withColumn(
        "row_number",
        row_number().over(dedupe_window),
    )
    .filter(col("row_number") == 1)
    .drop("row_number")
)

deduplicated_count = bronze_updates_df.count()

print(f"Incoming count: {incoming_count}")
print(f"Deduplicated count: {deduplicated_count}")
print(
    "Duplicates removed: "
    f"{incoming_count - deduplicated_count}"
)
print(
    "Bronze write mode: "
    f"{'FULL_SNAPSHOT' if full_snapshot else 'INCREMENTAL'}"
)

if (
    full_snapshot
    and deduplicated_count != incoming_count
):
    raise ValueError(
        "A full snapshot must contain one record per "
        "incident_report_number."
    )

bronze_updates_df.printSchema()

bronze_updates_df.select(
    "source_row_id",
    "incident_report_number",
    "crime_type",
    "dataset_id",
    "ingestion_run_id",
    "source_file"
).show(5, truncate=False)

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")

table_name = "lakehouse.bronze.crime_reports"

if full_snapshot:
    manifest_file = f"{raw_prefix}manifest.json"

    manifest = (
        spark.read
        .option("multiLine", "true")
        .json(manifest_file)
        .select(
            "status",
            "expected_record_count",
            "extracted_record_count",
        )
        .first()
    )

    if manifest is None:
        raise ValueError(
            "The Raw manifest could not be read."
        )

    if manifest["status"] != "SUCCESS":
        raise ValueError(
            "A full snapshot requires a SUCCESS "
            "Raw manifest."
        )

    if (
        manifest["expected_record_count"]
        != manifest["extracted_record_count"]
        or manifest["extracted_record_count"]
        != incoming_count
    ):
        raise ValueError(
            "The Raw manifest counts do not match "
            "the extracted full snapshot."
        )

    (
        bronze_updates_df.writeTo(table_name)
        .using("iceberg")
        .createOrReplace()
    )

    print("Bronze full snapshot replaced.")

elif spark.catalog.tableExists(table_name):
    target_versions_df = (
        spark.table(table_name)
        .select(
            col("incident_report_number").alias(
                "target_incident_report_number"
            ),
            col("source_updated_at").alias(
                "target_source_updated_at"
            ),
            col("source_version").alias(
                "target_source_version"
            ),
        )
    )

    records_requiring_merge_df = (
        bronze_updates_df.alias("source")
        .join(
            target_versions_df.alias("target"),
            col("source.incident_report_number")
            == col(
                "target.target_incident_report_number"
            ),
            "left",
        )
        .filter(
            col(
                "target.target_incident_report_number"
            ).isNull()
            | col(
                "target.target_source_updated_at"
            ).isNull()
            | (
                col("source.source_updated_at")
                > col(
                    "target.target_source_updated_at"
                )
            )
            | (
                (
                    col("source.source_updated_at")
                    == col(
                        "target.target_source_updated_at"
                    )
                )
                & ~col(
                    "source.source_version"
                ).eqNullSafe(
                    col("target.target_source_version")
                )
            )
        )
    )

    records_requiring_merge_count = (
        records_requiring_merge_df.count()
    )

    print(
        "Records requiring merge: "
        f"{records_requiring_merge_count}"
    )

    if records_requiring_merge_count == 0:
        print("No Bronze changes required.")

    else:
        bronze_updates_df.createOrReplaceTempView(
            "bronze_updates"
        )

        spark.sql(
            f"""
            MERGE INTO {table_name} AS target
            USING bronze_updates AS source
                ON target.incident_report_number
                    = source.incident_report_number

            WHEN MATCHED
                AND (
                    target.source_updated_at IS NULL
                    OR source.source_updated_at
                        > target.source_updated_at
                    OR (
                        source.source_updated_at
                            = target.source_updated_at
                        AND NOT (
                            source.source_version
                                <=> target.source_version
                        )
                    )
                )
                THEN UPDATE SET *

            WHEN NOT MATCHED
                THEN INSERT *
            """
        )

        print("Bronze merge completed.")

else:
    (
        bronze_updates_df.writeTo(table_name)
        .using("iceberg")
        .create()
    )

    print("Bronze table created.")
    
saved_bronze_df = spark.table(table_name)

bronze_state_df = (
    saved_bronze_df
    .select(
        col("incident_report_number").alias(
            "target_incident_report_number"
        ),
        col("source_updated_at").alias(
            "target_source_updated_at"
        ),
        col("source_version").alias(
            "target_source_version"
        ),
    )
)

unapplied_record_count = (
    bronze_updates_df.alias("source")
    .join(
        bronze_state_df.alias("target"),
        (
            col("source.incident_report_number")
            == col(
                "target.target_incident_report_number"
            )
        )
        & (
            (
                col("target.target_source_updated_at")
                > col("source.source_updated_at")
            )
            | (
                (
                    col(
                        "target.target_source_updated_at"
                    )
                    == col("source.source_updated_at")
                )
                & col(
                    "target.target_source_version"
                ).eqNullSafe(
                    col("source.source_version")
                )
            )
        ),
        "left_anti",
    )
    .count()
)

if unapplied_record_count > 0:
    raise ValueError(
        f"{unapplied_record_count} incoming records "
        "were not correctly applied to Bronze."
    )

saved_bronze_count = saved_bronze_df.count()

if (
    full_snapshot
    and saved_bronze_count != deduplicated_count
):
    raise ValueError(
        "The full Bronze snapshot count does not "
        "match the deduplicated Raw count: "
        f"bronze={saved_bronze_count}, "
        f"raw={deduplicated_count}"
    )

print(
    "Bronze source-to-target validation passed."
)

print(f"Saved Bronze count: {saved_bronze_count}")

spark.sql(
    f"""
    SELECT committed_at, operation
    FROM {table_name}.snapshots
    """
).show(truncate=False)

spark.stop()
