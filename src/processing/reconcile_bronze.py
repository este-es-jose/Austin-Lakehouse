import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_db = os.getenv("POSTGRES_DB")

if len(sys.argv) != 2:
    raise ValueError(
        "Usage: reconcile_bronze.py <run_id>"
    )

run_id = sys.argv[1]

spark = (
    SparkSession.builder
    .appName("Reconcile Source to Bronze")
    .master("local[4]")
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "http://minio:9000",
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        os.getenv("AWS_ACCESS_KEY_ID"),
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true",
    )
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false",
    )
    .config(
        "spark.sql.extensions",
        (
            "org.apache.iceberg.spark.extensions."
            "IcebergSparkSessionExtensions"
        ),
    )
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config(
        "spark.sql.catalog.lakehouse.type",
        "jdbc",
    )
    .config(
        "spark.sql.catalog.lakehouse.uri",
        (
            "jdbc:postgresql://postgres:5432/"
            f"{postgres_db}"
        ),
    )
    .config(
        "spark.sql.catalog.lakehouse.jdbc.user",
        postgres_user,
    )
    .config(
        "spark.sql.catalog.lakehouse.jdbc.password",
        postgres_password,
    )
    .config(
        "spark.sql.catalog.lakehouse."
        "jdbc.schema-version",
        "V1",
    )
    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        "s3://austin-crime-lakehouse/warehouse",
    )
    .config(
        "spark.sql.catalog.lakehouse.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )
    .config(
        "spark.sql.catalog.lakehouse.s3.endpoint",
        "http://minio:9000",
    )
    .config(
        "spark.sql.catalog.lakehouse."
        "s3.path-style-access",
        "true",
    )
    .config(
        "spark.sql.catalog.lakehouse.client.region",
        "us-east-1",
    )
    .config(
        "spark.sql.session.timeZone",
        "UTC",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

snapshot_file = (
    "s3a://austin-crime-lakehouse/"
    "reconciliation/crime_reports/"
    f"{run_id}/part_*.json"
)

source_keys_df = (
    spark.read
    .option("multiLine", "true")
    .json(snapshot_file)
    .withColumnRenamed(":id", "source_row_id")
    .withColumnRenamed(
        ":updated_at",
        "source_updated_at",
    )
    .select(
        "incident_report_number",
        "source_updated_at",
    )
)

source_keys_df = source_keys_df.cache()

bronze_keys_df = (
    spark.table(
        "lakehouse.bronze.crime_reports"
    )
    .select(
        "incident_report_number",
        "source_updated_at",
    )
    .cache()
)

source_count = source_keys_df.count()
bronze_count = bronze_keys_df.count()

source_unique_count = (
    source_keys_df
    .select("incident_report_number")
    .distinct()
    .count()
)

bronze_unique_count = (
    bronze_keys_df
    .select("incident_report_number")
    .distinct()
    .count()
)

source_null_ids = (
    source_keys_df
    .filter(
        col("incident_report_number").isNull()
    )
    .count()
)

bronze_null_ids = (
    bronze_keys_df
    .filter(
        col("incident_report_number").isNull()
    )
    .count()
)

print(f"Source count: {source_count}")
print(f"Bronze count: {bronze_count}")
print(
    "Source unique incident numbers: "
    f"{source_unique_count}"
)
print(
    "Bronze unique incident numbers: "
    f"{bronze_unique_count}"
)
print(
    "Source null incident numbers: "
    f"{source_null_ids}"
)
print(
    "Bronze null incident numbers: "
    f"{bronze_null_ids}"
)

if source_count != source_unique_count:
    raise ValueError(
        "Duplicate incident numbers were found in the "
        "source reconciliation snapshot."
    )

if bronze_count != bronze_unique_count:
    raise ValueError(
        "Duplicate incident numbers were found in Bronze."
    )

if source_null_ids > 0 or bronze_null_ids > 0:
    raise ValueError(
        "Null incident numbers were found."
    )

missing_from_bronze_count = (
    source_keys_df
    .select("incident_report_number")
    .join(
        bronze_keys_df.select(
            "incident_report_number"
        ),
        on="incident_report_number",
        how="left_anti",
    )
    .count()
)

missing_from_source_count = (
    bronze_keys_df
    .select("incident_report_number")
    .join(
        source_keys_df.select(
            "incident_report_number"
        ),
        on="incident_report_number",
        how="left_anti",
    )
    .count()
)

timestamp_mismatch_count = (
    source_keys_df.alias("source")
    .join(
        bronze_keys_df.alias("bronze"),
        on=(
            col("source.incident_report_number")
            == col(
                "bronze.incident_report_number"
            )
        ),
        how="inner",
    )
    .filter(
        ~col(
            "source.source_updated_at"
        ).eqNullSafe(
            col("bronze.source_updated_at")
        )
    )
    .count()
)

print(
    "Missing from Bronze: "
    f"{missing_from_bronze_count}"
)
print(
    "Missing from source: "
    f"{missing_from_source_count}"
)
print(
    "Timestamp mismatches: "
    f"{timestamp_mismatch_count}"
)

if (
    missing_from_bronze_count > 0
    or missing_from_source_count > 0
    or timestamp_mismatch_count > 0
):
    raise ValueError(
        "Source and Bronze reconciliation failed."
    )

print(
    "Full source-to-Bronze reconciliation passed."
)

source_keys_df.unpersist()
bronze_keys_df.unpersist()

spark.stop()
