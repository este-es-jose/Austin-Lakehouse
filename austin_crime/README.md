# Austin Crime dbt project

This dbt project transforms the Silver Austin crime table through Trino
into incident-level facts and monthly reporting marts.

## Models

```text
lakehouse.silver.crime_reports
    -> stg_crime_reports
    -> int_crime_reports_enriched
    -> fct_crime_incident
    -> mart_monthly_crime_trends
    -> mart_monthly_clearance_rates
```

Staging and intermediate models are views. The fact and mart models are
Iceberg tables partitioned by occurrence year.

## Run in Docker

The Airflow container includes dbt and uses the container-network Trino
profile in `airflow/dbt/profiles.yml`:

```powershell
docker compose exec airflow dbt build `
  --project-dir /opt/airflow/project/austin_crime `
  --profiles-dir /opt/airflow/dbt
```

Generate and serve dbt documentation:

```powershell
docker compose exec airflow dbt docs generate `
  --project-dir /opt/airflow/project/austin_crime `
  --profiles-dir /opt/airflow/dbt
```

## Tests

Schema tests enforce non-null and unique source and business keys.
Singular tests also verify that monthly incident totals match the fact
table and that clearance-status counts balance.

For the complete system architecture and operating instructions, see the
[root README](../README.md).
