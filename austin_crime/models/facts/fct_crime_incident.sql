{{
    config(
        materialized='table',
        properties={
            "partitioning": "ARRAY['year(occ_date)']"
        }
    )
}}

select
    incident_report_number,
    source_row_id,

    crime_type,
    category_description,
    ucr_category,
    ucr_code,

    occ_date,
    occ_date_time,
    rep_date,
    rep_date_time,
    clearance_date,

    clearance_status,
    is_cleared,
    is_family_violence,

    council_district,
    district,
    sector,
    census_block_group,
    location_type,

    reporting_delay_days,
    clearance_delay_days,
    has_date_sequence_issue,

    1 as incident_count,

    dataset_id,
    ingestion_run_id,
    ingested_at,
    source_created_at,
    source_updated_at

from {{ ref('int_crime_reports_enriched') }}