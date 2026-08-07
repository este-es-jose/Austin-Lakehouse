select
    *,

    case
        when rep_date >= occ_date
        then date_diff('day', occ_date, rep_date)
    end as reporting_delay_days,

    case
        when clearance_date >= rep_date
        then date_diff('day', rep_date, clearance_date)
    end as clearance_delay_days,

    coalesce(rep_date < occ_date, false)
        or coalesce(clearance_date < occ_date, false)
        or coalesce(clearance_date < rep_date, false)
        as has_date_sequence_issue

from {{ ref('stg_crime_reports') }}