with base as (
  select * 
  from {{ var('contracts') }}
),

final as (
    select *
    from base
)

select * 
from final