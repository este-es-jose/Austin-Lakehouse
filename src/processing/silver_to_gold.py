import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    trunc,
    when,
)


postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_db = os.getenv("POSTGRES_DB")

spark = (
    SparkSession.builder
    .appName("Silver to Gold Crime Summary")
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

silver_table = "lakehouse.silver.crime_reports"

silver_df = spark.table(silver_table)

monthly_df = silver_df.withColumn(
    "occ_month",
    trunc(col("occ_date"), "month")
)

gold_df = (
    monthly_df
    .groupBy(
        "occ_month",
        "crime_type",
        "council_district"
    )
    .agg(
        count("*").alias("incident_count"),
        sum(
            when(
                col("is_cleared").isNotNull(),
                1
            ).otherwise(0)
        ).alias("known_clearance_count"),
        sum(
            when(
                col("is_cleared") == True,
                1
            ).otherwise(0)
        ).alias("cleared_count"),
        sum(
            when(
                col("is_family_violence") == True,
                1
            ).otherwise(0)
        ).alias("family_violence_count")
    )
)

gold_df.printSchema()

gold_df.orderBy(
    col("occ_month").desc(),
    col("incident_count").desc()
).show(20, truncate=False)

print("Gold validation totals:")

gold_df.agg(
    count("*").alias("gold_row_count"),
    sum("incident_count").alias("total_incidents"),
    sum("known_clearance_count").alias(
        "total_known_clearance"
    ),
    sum("cleared_count").alias("total_cleared"),
    sum("family_violence_count").alias(
        "total_family_violence"
    )
).show(truncate=False)

gold_table = "lakehouse.gold.monthly_crime_summary"

spark.sql(
    "CREATE NAMESPACE IF NOT EXISTS lakehouse.gold"
)

(
    gold_df.writeTo(gold_table)
    .using("iceberg")
    .createOrReplace()
)

saved_gold_df = spark.table(gold_table)

print(f"Saved Gold count: {saved_gold_df.count()}")

spark.sql(
    f"""
    SELECT
        committed_at,
        operation
    FROM {gold_table}.snapshots
    """
).show(truncate=False)

spark.stop()