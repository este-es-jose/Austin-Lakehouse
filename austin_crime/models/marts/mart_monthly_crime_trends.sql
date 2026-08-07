{{
    config(
        materialized='table',
        properties={
            "partitioning": "ARRAY['year(occ_month)']"
        }
    )
}}

select
    cast(date_trunc('month', occ_date) as date) as occ_month,
    crime_type,
    council_district,

    sum(incident_count) as incident_count,

    count_if(is_cleared is not null)
        as known_clearance_count,

    count_if(is_cleared)
        as cleared_count,

    count_if(is_family_violence)
        as family_violence_count

from {{ ref('fct_crime_incident') }}

group by
    1,
    2,
    3