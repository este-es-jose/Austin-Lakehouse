import os

from pyspark.sql import SparkSession


postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_db = os.getenv("POSTGRES_DB")

spark = (
    SparkSession.builder
    .appName("Inspect Bronze Crime Reports")
    .master("local[4]")
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
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

table_name = "lakehouse.bronze.crime_reports"

print("Bronze key summary:")

spark.sql(
    f"""
    SELECT
        COUNT(*) AS record_count,
        COUNT(DISTINCT source_row_id) AS distinct_source_row_ids,
        SUM(
            CASE
                WHEN source_row_id IS NULL OR TRIM(source_row_id) = '' THEN 1
                ELSE 0
            END
        ) AS missing_source_row_ids,
        COUNT(DISTINCT incident_report_number)
            AS distinct_incident_report_numbers,
        SUM(
            CASE
                WHEN incident_report_number IS NULL
                    OR TRIM(incident_report_number) = '' THEN 1
                ELSE 0
            END
        ) AS missing_incident_report_numbers
    FROM {table_name}
    """
).show(truncate=False)

print("Most frequently repeated incident report numbers:")

spark.sql(
    f"""
    SELECT
        incident_report_number,
        COUNT(*) AS record_count
    FROM {table_name}
    WHERE incident_report_number IS NOT NULL
        AND TRIM(incident_report_number) <> ''
    GROUP BY incident_report_number
    HAVING COUNT(*) > 1
    ORDER BY record_count DESC, incident_report_number
    LIMIT 10
    """
).show(truncate=False)

print("Date field coverage:")

spark.sql(
    f"""
    SELECT
        MIN(occ_date) AS earliest_occ_date,
        MAX(occ_date) AS latest_occ_date,
        COUNT(*) - COUNT(occ_date) AS missing_occ_date,

        MIN(rep_date) AS earliest_rep_date,
        MAX(rep_date) AS latest_rep_date,
        COUNT(*) - COUNT(rep_date) AS missing_rep_date,

        COUNT(*) - COUNT(clearance_date) AS missing_clearance_date,
        COUNT(*) - COUNT(source_created_at) AS missing_source_created_at,
        COUNT(*) - COUNT(source_updated_at) AS missing_source_updated_at
    FROM {table_name}
    """
).show(truncate=False)

print("Clearance dates by status:")

spark.sql(
    f"""
    SELECT
        COALESCE(clearance_status, 'MISSING') AS clearance_status,
        COUNT(*) AS record_count,
        COUNT(clearance_date) AS records_with_clearance_date,
        COUNT(*) - COUNT(clearance_date) AS records_without_clearance_date
    FROM {table_name}
    GROUP BY COALESCE(clearance_status, 'MISSING')
    ORDER BY record_count DESC
    """
).show(truncate=False)

print("Family violence values:")

spark.sql(
    f"""
    SELECT
        COALESCE(family_violence, 'MISSING') AS family_violence,
        COUNT(*) AS record_count
    FROM {table_name}
    GROUP BY COALESCE(family_violence, 'MISSING')
    ORDER BY record_count DESC
    """
).show(truncate=False)

print("Date samples:")

spark.sql(
    f"""
    SELECT
        occ_date,
        occ_date_time,
        rep_date,
        rep_date_time,
        clearance_date,
        source_created_at,
        source_updated_at
    FROM {table_name}
    WHERE clearance_date IS NOT NULL
    LIMIT 10
    """
).show(truncate=False)

spark.stop()
