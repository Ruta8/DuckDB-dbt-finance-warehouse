-- Fails if account-month end MRR doesn't match sum of subscription-month MRR
with acct as (
  select
    account_id,
    month_start_date,
    mrr_end_mrr
  from {{ ref('fct_account_month') }}
),

subs as (
  select
    account_id,
    month_start_date,
    sum(coalesce(mrr_amount, 0)) as subs_end_mrr
  from {{ ref('fct_subscription_month') }}
  group by 1, 2
),

bad as (
  select
    a.account_id,
    a.month_start_date,
    a.mrr_end_mrr,
    coalesce(s.subs_end_mrr, 0) as subs_end_mrr
  from acct a
  left join subs s
    on s.account_id = a.account_id
   and s.month_start_date = a.month_start_date
  where abs(a.mrr_end_mrr - coalesce(s.subs_end_mrr, 0)) > 0.0001
)

select *
from bad
