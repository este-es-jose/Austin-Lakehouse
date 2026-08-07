select *
from {{ source('silver', 'crime_reports') }}