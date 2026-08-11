# Architecture and design

## System boundary

The lakehouse ingests the City of Austin Crime Reports dataset
(`fdj4-gpfu`). Socrata is an external system, so the pipeline treats its
records, timestamps, paging behavior, and schema as source-controlled
inputs rather than guarantees.

The current release runs on one machine with Docker Compose. Its service
boundaries mirror a larger deployment without claiming cloud-level
availability or scale.

## Data flow

1. The Python extractor reads the last successful `:updated_at`
   watermark from PostgreSQL.
2. It asks Socrata for the changed-record count and maximum update time.
   That maximum becomes the fixed upper bound for the extraction window.
3. Changed records are ordered by `:updated_at` and `:id`, downloaded in
   pages of 10,000, and written as JSON parts with a manifest.
4. The upload step validates file sizes, stores the batch under
   `raw/crime_reports/<run_id>/` in MinIO, and records each raw object in
   PostgreSQL.
5. Spark renames Socrata system fields, adds ingestion metadata, rejects
   missing business or source keys, and deduplicates the incoming batch
   by `incident_report_number`.
6. A full snapshot replaces Bronze. An incremental batch uses an Iceberg
   `MERGE` to update changed incidents and insert new incidents.
7. Spark rebuilds Silver with controlled dates, timestamps, integers,
   booleans, and standardized categorical values.
8. Spark creates a monthly Gold aggregate. dbt independently builds a
   fact table and reporting marts through Trino.
9. Validation compares record totals across Bronze, Silver, Spark Gold,
   the dbt fact, and the dbt mart. It also performs a bidirectional row
   comparison between Spark Gold and the matching dbt output.
10. Only after validation succeeds does finalization advance the source
    watermark and mark the pipeline run successful.

## Load-mode safety

The extractor calculates the changed count before downloading records.

| Condition | Mode | Reason |
| --- | --- | --- |
| No changed records | `NO_CHANGES` | Avoid unnecessary storage and table scans. |
| Changed count is below 80% of source count | `INCREMENTAL` | Process only created or updated records. |
| Changed count equals source count | `FULL_SNAPSHOT` | Treat a complete source republish as a replacement snapshot. |
| Changed count is at least 80% but below 100% | `REVIEW_REQUIRED` | Prevent a suspicious bulk update from being processed automatically. |

Incremental queries use an exclusive lower watermark and an inclusive,
fixed upper bound:

```text
:updated_at > previous_watermark
AND :updated_at <= window_end
```

Records sharing a timestamp are ordered by source ID for stable paging.
The upper bound prevents records changed during a long extraction from
moving between pages; those later changes are left for the next run.

### Observed source publication behavior

The watermark identifies a new source publication, but it cannot always
identify individual business-record changes. A comparison of the August
3 and August 10, 2026 Iceberg snapshots found:

| Observation | Records |
| --- | ---: |
| Inserted incidents | 1,762 |
| Removed incidents | 37 |
| Matched incidents with changed business values | 823 |
| Matched incidents with unchanged business values | 2,660,476 |
| Matched incidents with changed Socrata timestamps | 2,661,299 |
| Matched incidents with changed Socrata versions | 2,661,299 |
| Matched incidents with changed Socrata row IDs | 2,661,299 |

The City dataset was republished as a complete source snapshot. Socrata
regenerated `:id`, `:version`, and `:updated_at` for every matched row,
including rows whose business values did not change. Consequently, these
system fields cannot provide row-level CDC for this dataset.

`incident_report_number` is the stable cross-publication key. A source
publication currently requires a full snapshot to discover business
changes and removals. Incremental merging would still require downloading
the complete publication and would not remove records absent from the
new source snapshot.

## Storage and table design

### Raw

Raw JSON parts and their manifest are preserved by run ID. The raw layer
is the replayable source for downstream processing and is not edited in
place.

### Bronze

`lakehouse.bronze.crime_reports` remains close to the source schema and
adds:

- `dataset_id`
- `ingestion_run_id`
- `ingested_at`
- `source_file`
- renamed Socrata system columns

`incident_report_number` is the merge key. `source_row_id` is retained as
the Socrata-generated identity and is also validated for uniqueness
within a publication, but it is not stable between bulk publications.
Incoming duplicates are resolved using source update/version metadata.

### Silver

`lakehouse.silver.crime_reports` standardizes dates, timestamps,
booleans, clearance values, and council district types. It is partitioned
with Iceberg's `year(occ_date)` transform.

### Gold and dbt

Spark writes `lakehouse.gold.monthly_crime_summary`. dbt builds:

- `dbt_dev.stg_crime_reports`
- `dbt_dev.int_crime_reports_enriched`
- `dbt_dev.fct_crime_incident`
- `dbt_dev.mart_monthly_crime_trends`
- `dbt_dev.mart_monthly_clearance_rates`

The deliberate overlap between Spark Gold and the monthly dbt mart makes
it possible to validate two independently expressed transformations.

## Control plane

The `ops_control` PostgreSQL schema contains:

- `pipeline_run`: overall state, timing, and failure information
- `extract_batch`: source, paging, record counts, and raw paths
- `raw_file_manifest`: object-level record counts and sizes
- `source_watermark`: the last committed source position
- `pipeline_run_summary`: joined operational view for investigation

PostgreSQL also backs the Iceberg JDBC catalog. Analytical crime records
remain in Iceberg files in MinIO rather than being copied into PostgreSQL.

## Orchestration and reconciliation

The daily `austin_crime_pipeline` DAG coordinates extraction, upload,
Bronze, Silver, Gold, dbt, validation, and finalization. Business logic
lives in reusable Python and Spark scripts rather than in the DAG.

The monthly `austin_crime_reconciliation` DAG is intentionally separate.
It downloads a source key/timestamp snapshot and compares it with Bronze
to detect missing keys, extra keys, null or duplicate identities, and
timestamp mismatches. This full comparison is an occasional correctness
check, not the normal ingestion strategy.

## Serving and observability

Trino is the SQL access layer for Iceberg. dbt uses Trino for models and
tests, and Superset queries the dbt reporting schema through Trino.

Airflow sends StatsD metrics to `statsd-exporter`. Prometheus scrapes the
exporter, and Grafana queries Prometheus. PostgreSQL operational records
provide a complementary audit trail for pipeline-level investigations.

## Intentional boundaries

The initial project brief described a larger target architecture. The
following items are not implemented in this release and should not be
presented as completed features:

- Apache Polaris
- automatic schema-drift classification and quarantine
- a slowly changing or full historical Silver table
- source deletion handling
- automatic repair from reconciliation results
- ML forecasting and MLflow
- cloud deployment, Kafka, Flink, or Kubernetes

The JDBC catalog and batch architecture are deliberate first-release
choices. Future additions should be driven by a concrete requirement or
measured operational problem.

One possible optimization is a source-side fingerprint calculated from
the stable business columns. If Socrata can return
`incident_report_number` plus a reliable hash, the pipeline could compare
that lightweight index with Bronze and fetch full payloads only for new
or changed records. This remains a hypothesis to validate, not an
implemented capability.
