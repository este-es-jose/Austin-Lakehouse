select *
from {{ ref('mart_monthly_clearance_rates') }}

where incident_count
        <> known_clearance_count + unknown_clearance_count

   or known_clearance_count
        <> cleared_count + not_cleared_count

   or clearance_rate < 0
   or clearance_rate > 1