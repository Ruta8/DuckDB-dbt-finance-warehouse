{{ config(materialized='view') }}

select *
from (
  select
    *,
    row_number() over (partition by account_id order by ingested_at desc) as rn
  from {{ ref('stg_accounts') }}
)
where rn = 1

