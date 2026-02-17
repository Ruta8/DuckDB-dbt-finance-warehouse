-- Fails if waterfall math doesn't reconcile
with w as (
  select
    month_start_date,
    begin_mrr,
    end_mrr,
    new_mrr,
    reactivation_mrr,
    expansion_mrr,
    contraction_mrr,
    churn_mrr
  from {{ ref('mart_mrr_waterfall_month') }}
),

bad as (
  select
    *,
    (begin_mrr + new_mrr + reactivation_mrr + expansion_mrr - contraction_mrr - churn_mrr) as calc_end_mrr
  from w
  where abs(
    (begin_mrr + new_mrr + reactivation_mrr + expansion_mrr - contraction_mrr - churn_mrr) - end_mrr
  ) > 0.0001
)

select *
from bad
