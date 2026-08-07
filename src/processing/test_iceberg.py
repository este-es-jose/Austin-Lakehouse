import os

from pyspark.sql import SparkSession


postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_db = os.getenv("POSTGRES_DB")

spark = (
    SparkSession.builder
    .appName("Rename Bronze Crime Reports Table")
    .master("local[2]")
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
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.sql(
    """
    ALTER TABLE lakehouse.bronze.crime_reports_test
    RENAME TO bronze.crime_reports
    """
)

print("Bronze tables:")

spark.sql(
    "SHOW TABLES IN lakehouse.bronze"
).show(truncate=False)

spark.stop()