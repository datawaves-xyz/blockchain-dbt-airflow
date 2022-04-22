with base as (
  select * 
  from {{ var('blocks') }}
),

final as (
    select *
    from base
)

select * 
from final