# Operations runbook

Commands in this runbook use PowerShell and assume the current directory
is the repository root.

## First-time initialization

### 1. Configure the environment

```powershell
Copy-Item .env.example .env
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
```

Edit `.env` and replace every placeholder credential. Generate a long,
random `SUPERSET_SECRET_KEY`. Keep `.env` local; it is ignored by Git.

Validate the configuration before starting services:

```powershell
docker compose config --quiet
```

### 2. Start the core services

Start PostgreSQL and MinIO first:

```powershell
docker compose up -d postgres minio
docker compose ps
```

Initialize the operational schema:

```powershell
Get-Content .\infra\postgres\ops_control.sql |
  docker compose exec -T postgres sh -c `
    'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Create the MinIO bucket:

```powershell
docker compose exec minio sh -c `
  'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && mc mb --ignore-existing local/austin-crime-lakehouse'
```

### 3. Initialize Superset

The example configuration uses a separate PostgreSQL database named
`superset`. Create it once:

```powershell
docker compose exec postgres sh -c `
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE DATABASE superset;"'
```

If PostgreSQL reports that the database already exists, do not recreate
or delete it.

Build and start the complete platform:

```powershell
docker compose up -d --build
docker compose ps
```

Initialize Superset metadata and create the administrator interactively:

```powershell
docker compose exec superset superset db upgrade
docker compose exec superset superset fab create-admin
docker compose exec superset superset init
```

The Superset Trino connection used by this project is:

```text
trino://superset@trino:8080/lakehouse/dbt_dev
```

The exported dashboard assets are stored under
`infra/superset/assets/austin_crime_overview/`. They are a versioned
backup of the dashboard definition, not an automatic provisioning step.

### 4. Bootstrap the first data snapshot

The scheduled DAG requires an existing Bronze baseline and source
watermark. The first full extraction is therefore a deliberate manual
bootstrap.

Activate the virtual environment and run the full extractor:

```powershell
.\.venv\Scripts\Activate.ps1
python .\src\ingestion\extract_raw.py
```

The command prints a raw directory ending in a run ID such as
`20260730T160956Z`. Upload that completed run:

```powershell
python .\src\storage\upload_raw.py <run-id>
```

Set the Spark dependency string once in the current PowerShell session:

```powershell
$sparkPackages = "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0,org.postgresql:postgresql:42.7.10"
```

Create the first Bronze snapshot:

```powershell
docker compose exec spark `
  /opt/spark/bin/spark-submit `
  --driver-memory 6g `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages $sparkPackages `
  /opt/spark/work-dir/raw_to_bronze.py `
  <run-id> `
  --full-snapshot
```

Build Silver and Gold:

```powershell
docker compose exec spark `
  /opt/spark/bin/spark-submit `
  --driver-memory 6g `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages $sparkPackages `
  /opt/spark/work-dir/bronze_to_silver.py

docker compose exec spark `
  /opt/spark/bin/spark-submit `
  --driver-memory 6g `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages $sparkPackages `
  /opt/spark/work-dir/silver_to_gold.py
```

Build dbt models and validate all layers:

```powershell
docker compose exec airflow dbt build `
  --no-partial-parse `
  --project-dir /opt/airflow/project/austin_crime `
  --profiles-dir /opt/airflow/dbt `
  --target-path /tmp/austin-crime-dbt-target

docker compose exec airflow python `
  /opt/airflow/project/src/validation/validate_pipeline.py
```

Finally, query Bronze for its maximum `source_updated_at`, read the full
extraction's `pipeline_run_id` from its manifest, and seed
`ops_control.source_watermark`. This is a one-time control-plane action;
verify both values before executing the insert:

```powershell
docker compose exec trino trino `
  --execute 'SELECT max(source_updated_at) FROM lakehouse.bronze.crime_reports'
```

```powershell
@'
INSERT INTO ops_control.source_watermark (
    dataset_id,
    watermark_column,
    watermark_value,
    last_successful_pipeline_run_id
)
VALUES (
    'fdj4-gpfu',
    ':updated_at',
    '<maximum-source-updated-at>',
    '<full-extraction-pipeline-run-id>'
)
ON CONFLICT (dataset_id)
DO UPDATE SET
    watermark_value = EXCLUDED.watermark_value,
    last_successful_pipeline_run_id =
        EXCLUDED.last_successful_pipeline_run_id,
    updated_at = current_timestamp;
'@ | docker compose exec -T postgres sh -c `
  'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Do not advance the watermark unless the full extraction and every
downstream validation completed successfully.

## Routine operation

Start or update the platform:

```powershell
docker compose up -d --build
docker compose ps
```

Stop containers while preserving all named volumes:

```powershell
docker compose stop
```

Remove containers and the Compose network while preserving data:

```powershell
docker compose down
```

Do not add `--volumes` unless a complete local reset is intentional.

## Airflow

Open <http://localhost:8082>. Retrieve the generated Airflow standalone
credentials with:

```powershell
docker compose exec airflow `
  cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

The primary DAG is `austin_crime_pipeline`. A normal no-change run should
record `NO_CHANGES / SUCCESS`, process zero records, and skip Bronze,
Silver, Gold, and dbt tasks.

The `austin_crime_reconciliation` DAG is a monthly full comparison. It is
expected to scan the complete source and Bronze datasets, so it should
not replace the daily incremental DAG.

## Health and logs

Inspect service status:

```powershell
docker compose ps
```

Follow one service's logs:

```powershell
docker compose logs --follow --tail 200 airflow
```

Replace `airflow` with `spark`, `trino`, `postgres`, `minio`, `superset`,
`prometheus`, or `grafana` as needed.

Check the latest pipeline records:

```powershell
@'
SELECT
    pipeline_run_id,
    run_type,
    pipeline_status,
    batch_status,
    pages_downloaded,
    record_count,
    pipeline_started_at,
    pipeline_finished_at,
    watermark_value,
    is_last_successful_run
FROM ops_control.pipeline_run_summary
ORDER BY pipeline_started_at DESC
LIMIT 10;
'@ | docker compose exec -T postgres sh -c `
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Check the committed watermark:

```powershell
@'
SELECT
    dataset_id,
    watermark_value,
    last_successful_pipeline_run_id,
    updated_at
FROM ops_control.source_watermark;
'@ | docker compose exec -T postgres sh -c `
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

## Validation and tests

Fast local checks:

```powershell
python -m compileall -q src airflow/dags tests
python -m pytest
dbt parse --project-dir austin_crime --profiles-dir airflow/dbt
docker compose config --quiet
git diff --check
```

Validate the live data layers:

```powershell
docker compose exec airflow python `
  /opt/airflow/project/src/validation/validate_pipeline.py
```

The isolated Bronze test is documented in
[`tests/integration/README.md`](../tests/integration/README.md). The same
test can be started manually from GitHub Actions using the
`Bronze integration` workflow.

## Common failures

### Incremental run reports nearly every source record as changed

Do not assume that the extractor ignored its watermark. First inspect the
logged source count, changed count, percentage, window end, and selected
load mode. A source-wide update can legitimately make every record newer
than the watermark. Complete source changes use `FULL_SNAPSHOT`; a
suspicious partial bulk change is blocked as `REVIEW_REQUIRED`.

### Trino hostname fails from the Windows host

The hostname `trino` resolves only inside the Compose network. Host-run
programs must use `localhost`; container-run programs use `trino`.

### Spark runs out of Java heap

Confirm Docker Desktop has enough memory and keep the Spark driver at
6 GB for the full dataset. Avoid adding extra full-table actions to the
jobs unless the result is required for validation.

### No downstream Airflow tasks run

Inspect the extraction result. If `has_changes` is false, downstream
processing is intentionally skipped. If it is true, inspect the first
failed task and the corresponding PostgreSQL pipeline record before
retrying.

### dbt fails while parsing adapter macros

Airflow must not use the host-generated `austin_crime/target` cache. The
orchestrated dbt command disables partial parsing and writes generated
artifacts to `/tmp/austin-crime-dbt-target` inside the container. If an
older DAG run failed with a `dbt_trino://macros/...` `KeyError`, deploy
the corrected DAG and trigger a new run rather than retrying only the
failed task. The failed pipeline record is retained for auditing, and
the unchanged watermark makes the new run safely replay the source
window.
