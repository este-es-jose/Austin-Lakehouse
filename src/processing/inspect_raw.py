import os

from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("Inspect Raw Crime Reports")
    .master("local[*]")
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
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_file = (
    "s3a://austin-crime-lakehouse/"
    "raw/crime_reports/"
    "20260720T173258Z/"
    "part_*.json"
)

df = (
    spark.read
    .option("multiLine", "true")
    .json(raw_file)
)

df.printSchema()

print(f"Record count: {df.count()}")

df.select(
    "incident_report_number",
    "crime_type",
    "occ_date"
).show(5, truncate=False)

spark.stop()