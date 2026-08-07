with fact_total as (
    select
        sum(incident_count) as incident_count
    from {{ ref('fct_crime_incident') }}
),

mart_total as (
    select
        sum(incident_count) as incident_count
    from {{ ref('mart_monthly_crime_trends') }}
)

select
    fact_total.incident_count as fact_incidents,
    mart_total.incident_count as mart_incidents

from fact_total
cross join mart_total

where fact_total.incident_count
    <> mart_total.incident_count