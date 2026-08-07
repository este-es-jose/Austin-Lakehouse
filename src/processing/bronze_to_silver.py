import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    regexp_replace,
    to_date,
    to_timestamp,
    to_timestamp_ntz,
    trim,
    upper,
    when,
    years,
)


postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_db = os.getenv("POSTGRES_DB")

spark = (
    SparkSession.builder
    .appName("Bronze to Silver Crime Reports")
    .master("local[4]")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        "spark.sql.catalog.lakehouse.type",
        "jdbc"
    )
    .config(
        "spark.sql.catalog.lakehouse.uri",
        f"jdbc:postgresql://postgres:5432/{postgres_db}"
    )
    .config(
        "spark.sql.catalog.lakehouse.jdbc.user",
        postgres_user
    )
    .config(
        "spark.sql.catalog.lakehouse.jdbc.password",
        postgres_password
    )
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

bronze_table = "lakehouse.bronze.crime_reports"
silver_table = "lakehouse.silver.crime_reports"

bronze_df = spark.table(bronze_table)

silver_df = (
    bronze_df
    .withColumn(
        "occ_date",
        to_date(
            col("occ_date"),
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
    )
    .withColumn(
        "occ_date_time",
        to_timestamp_ntz(
            regexp_replace(
                trim(col("occ_date_time")),
                r"\s+",
                " "
            ),
            lit("MM/dd/yyyy HH:mm")
        )
    )
    .withColumn(
        "rep_date",
        to_date(
            col("rep_date"),
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
    )
    .withColumn(
        "rep_date_time",
        to_timestamp_ntz(
            regexp_replace(
                trim(col("rep_date_time")),
                r"\s+",
                " "
            ),
            lit("MM/dd/yyyy HH:mm")
        )
    )
    .withColumn(
        "clearance_date",
        to_date(
            col("clearance_date"),
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
    )
    .withColumn(
        "source_created_at",
        to_timestamp(
            col("source_created_at"),
            "yyyy-MM-dd'T'HH:mm:ss.SSSX"
        )
    )
    .withColumn(
        "source_updated_at",
        to_timestamp(
            col("source_updated_at"),
            "yyyy-MM-dd'T'HH:mm:ss.SSSX"
        )
    )
    .withColumn(
        "is_family_violence",
        when(
            upper(trim(col("family_violence"))) == "Y",
            True
        ).when(
            upper(trim(col("family_violence"))) == "N",
            False
        )
    )
    .withColumn(
        "clearance_status",
        upper(trim(col("clearance_status")))
    )
    .withColumn(
        "is_cleared",
        when(
            col("clearance_status").isin("C", "O"),
            True
        ).when(
            col("clearance_status") == "N",
            False
        )
    )
    .withColumn(
        "council_district",
        col("council_district").cast("integer")
    )
    .drop("family_violence")
)

spark.sql(
    "CREATE NAMESPACE IF NOT EXISTS lakehouse.silver"
)

(
    silver_df.writeTo(silver_table)
    .using("iceberg")
    .partitionedBy(years("occ_date"))
    .createOrReplace()
)

saved_silver_df = spark.table(silver_table)

saved_silver_df.printSchema()

print(f"Saved Silver count: {saved_silver_df.count()}")

spark.sql(
    f"""
    SELECT
        partition,
        record_count,
        file_count
    FROM {silver_table}.partitions
    ORDER BY partition
    """
).show(30, truncate=False)

spark.stop()