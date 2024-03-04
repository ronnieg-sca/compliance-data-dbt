with pos_cases_source as (
    select * from {{source ('app_compliance', 'pos_cases') }}
)
select
    case_token,
    queue
from
    pos_cases_source