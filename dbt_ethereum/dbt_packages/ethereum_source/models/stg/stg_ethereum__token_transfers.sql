{{ config(enabled=var('using_token_transfers', True)) }}

with base as (
  select * 
  from {{ var('token_transfers') }}
),

final as (
    select *
    from base
)

select * 
from final