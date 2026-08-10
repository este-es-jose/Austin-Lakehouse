# Bronze Incremental Merge Fixture

This test validates the Bronze incremental merge using small local JSON
fixtures and an isolated Iceberg table.

It tests:

- creation of an initial Bronze table
- incoming-batch deduplication
- updating an existing incident
- inserting a new incident
- ignoring an unchanged incident
- idempotent batch reprocessing

## Safety

The test uses:

```text
Raw fixtures:
file:///opt/spark/tests/fixtures/raw/crime_reports

Iceberg namespace:
lakehouse.integration_test
```

It does not modify:

```text
lakehouse.bronze.crime_reports
ops_control.source_watermark
raw/crime_reports in MinIO
```

## Prerequisites

The Docker Compose services must be running.

Recreate Spark after adding or changing the fixture mount:

```powershell
docker compose up -d --force-recreate spark
```

Verify the fixture files:

```powershell
docker compose exec spark `
  find /opt/spark/tests/fixtures/raw/crime_reports `
  -maxdepth 3 `
  -type f
```

## Prepare the test table

```powershell
docker compose exec trino trino `
  --execute "CREATE SCHEMA IF NOT EXISTS lakehouse.integration_test"
```

```powershell
docker compose exec trino trino `
  --execute "DROP TABLE IF EXISTS lakehouse.integration_test.crime_reports"
```

## Spark packages

```powershell
$sparkPackages = "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0,org.postgresql:postgresql:42.7.10"
```

## Load the initial fixture

```powershell
docker compose exec `
  -e RAW_CRIME_REPORTS_ROOT=file:///opt/spark/tests/fixtures/raw/crime_reports `
  -e BRONZE_NAMESPACE=lakehouse.integration_test `
  spark `
  /opt/spark/bin/spark-submit `
  --driver-memory 6g `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages $sparkPackages `
  /opt/spark/work-dir/raw_to_bronze.py `
  fixture_initial
```

Expected result:

```text
Incoming count: 2
Deduplicated count: 2
Duplicates removed: 0
Bronze table created.
Saved Bronze count: 2
```

## Load the incremental fixture

```powershell
docker compose exec `
  -e RAW_CRIME_REPORTS_ROOT=file:///opt/spark/tests/fixtures/raw/crime_reports `
  -e BRONZE_NAMESPACE=lakehouse.integration_test `
  spark `
  /opt/spark/bin/spark-submit `
  --driver-memory 6g `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages $sparkPackages `
  /opt/spark/work-dir/raw_to_bronze.py `
  fixture_incremental
```

Expected result:

```text
Incoming count: 4
Deduplicated count: 3
Duplicates removed: 1
Records requiring merge: 2
Bronze merge completed.
Saved Bronze count: 3
```

## Validate the saved table

```powershell
docker compose exec trino trino `
  --execute "SELECT
      incident_report_number,
      crime_type,
      source_version,
      ingestion_run_id
    FROM lakehouse.integration_test.crime_reports
    ORDER BY incident_report_number"
```

Expected rows:

```text
FIXTURE-100 | THEFT               | 1 | fixture_initial
FIXTURE-200 | BURGLARY OF VEHICLE | 2 | fixture_incremental
FIXTURE-300 | AUTO THEFT          | 1 | fixture_incremental
```

## Validate idempotency

Run the incremental fixture command a second time.

Expected result:

```text
Records requiring merge: 0
No Bronze changes required.
Saved Bronze count: 3
```

The rerun should not create another Iceberg snapshot.

Inspect the snapshots:

```powershell
@'
SELECT
    committed_at,
    operation
FROM lakehouse.integration_test."crime_reports$snapshots"
ORDER BY committed_at;
'@ | docker compose exec -T trino trino
```

Expected operations:

```text
append
overwrite
```

## Cleanup

Drop only the isolated fixture table:

```powershell
docker compose exec trino trino `
  --execute "DROP TABLE lakehouse.integration_test.crime_reports"
```

Remove the empty namespace:

```powershell
docker compose exec trino trino `
  --execute "DROP SCHEMA lakehouse.integration_test"
```

Do not use `CASCADE`.
