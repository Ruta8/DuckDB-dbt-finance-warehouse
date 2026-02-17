-- For EOM snapshot (active on last day of month):
-- If subscription_end_date is before the last day of the month,
-- that subscription should not contribute MRR in that month

with f as (
  select
    subscription_id,
    month_start_date,
    subscription_end_date,
    mrr_amount
  from {{ ref('fct_subscription_month') }}
  where subscription_end_date is not null
),

bad as (
  select
    *,
    (month_start_date + interval 1 month - interval 1 day)::date as last_day_of_month
  from f
  where subscription_end_date < (month_start_date + interval 1 month - interval 1 day)::date
    and coalesce(mrr_amount, 0) <> 0
)

select *
from bad
