with base as (
  select * 
  from {{ var('transactions') }}
),

final as (
    select *
    from base
)

select * 
from final