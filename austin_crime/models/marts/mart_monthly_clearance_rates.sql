{{
    config(
        materialized='table',
        properties={
            "partitioning": "ARRAY['year(occ_month)']"
        }
    )
}}

with monthly_counts as (
    select
        cast(date_trunc('month', occ_date) as date) as occ_month,
        council_district,

        sum(incident_count) as incident_count,

        count_if(is_cleared is not null)
            as known_clearance_count,

        count_if(is_cleared)
            as cleared_count,

        count_if(is_cleared = false)
            as not_cleared_count,

        count_if(is_cleared is null)
            as unknown_clearance_count

    from {{ ref('fct_crime_incident') }}

    group by
        1,
        2
)

select
    *,

    cast(cleared_count as double)
        / nullif(known_clearance_count, 0)
        as clearance_rate

from monthly_counts