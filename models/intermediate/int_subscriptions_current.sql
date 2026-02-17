{{ config(materialized='view') }}

select *
from (
  select
    *,
    row_number() over (partition by subscription_id order by ingested_at desc) as rn
  from {{ ref('stg_subscriptions') }}
)
where rn = 1

