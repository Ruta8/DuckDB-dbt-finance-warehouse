{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['account_id','month_start_date']
) }}

with months as (

  select distinct
    month_start_date
  from {{ ref('dim_date') }}
  where is_month_start = true

  {% if is_incremental() %}
    and month_start_date >= (
      select date_trunc(
        'month',
        max(month_start_date) - interval '{{ var("reprocess_months", 2) }} month'
      )::date
      from {{ this }}
    )
  {% endif %}

),

account_bounds as (

  select
    account_id,
    min(month_start_date) as first_month,
    max(month_start_date) as last_month
  from {{ ref('fct_subscription_month') }}
  group by 1

),

account_month_spine as (

  select
    ab.account_id,
    m.month_start_date
  from account_bounds ab
  join months m
    on m.month_start_date between ab.first_month
                         and (ab.last_month + interval 1 month)

),

monthly_mrr as (

  select
    account_id,
    month_start_date,
    sum(coalesce(mrr_amount, 0)) as mrr_end_mrr
  from {{ ref('fct_subscription_month') }}
  group by 1, 2

),

spined as (

  select
    s.account_id,
    s.month_start_date,
    coalesce(mm.mrr_end_mrr, 0) as mrr_end_mrr
  from account_month_spine s
  left join monthly_mrr mm
    on mm.account_id = s.account_id
   and mm.month_start_date = s.month_start_date

),

with_begin as (

  select
    account_id,
    month_start_date,
    mrr_end_mrr,
    coalesce(
      lag(mrr_end_mrr) over (partition by account_id order by month_start_date),
      0
    ) as mrr_begin_mrr
  from spined

),

with_flags as (

  select
    *,
    (mrr_end_mrr - mrr_begin_mrr) as mrr_delta,

    coalesce(
      max(case when mrr_end_mrr > 0 then 1 else 0 end)
        over (
          partition by account_id
          order by month_start_date
          rows between unbounded preceding and 1 preceding
        ),
      0
    ) = 1 as has_paid_before

  from with_begin

),

classified as (

  select
    *,
    case
      when mrr_begin_mrr = 0 and mrr_end_mrr > 0 and has_paid_before = false then 'new'
      when mrr_begin_mrr = 0 and mrr_end_mrr > 0 and has_paid_before = true  then 'reactivation'
      when mrr_begin_mrr > 0 and mrr_end_mrr = 0 then 'churn'
      when mrr_begin_mrr > 0 and mrr_end_mrr > 0 and mrr_delta > 0 then 'upgrade'
      when mrr_begin_mrr > 0 and mrr_end_mrr > 0 and mrr_delta < 0 then 'downgrade'
      when mrr_begin_mrr = 0 and mrr_end_mrr = 0 then 'zero'
      else 'no_change'
    end as movement_type

  from with_flags

),

acct_dim as (
  select
    account_id,
    account_key,

    date_trunc('month', valid_from)::date as valid_from_month,
    case when valid_to is null then null else date_trunc('month', valid_to)::date end as valid_to_month,

    min(date_trunc('month', valid_from)::date) over (partition by account_id) as first_valid_from_month
  from {{ ref('dim_account') }}
),

final as (

  select
    ad.account_key,
    c.account_id,
    c.month_start_date,

    c.mrr_begin_mrr,
    c.mrr_end_mrr,
    c.mrr_delta,
    c.movement_type,

    case when c.movement_type = 'new'          then c.mrr_end_mrr else 0 end as new_mrr,
    case when c.movement_type = 'reactivation' then c.mrr_end_mrr else 0 end as reactivation_mrr,
    case when c.movement_type = 'upgrade'      then c.mrr_delta   else 0 end as upgrade_mrr,
    case when c.movement_type = 'downgrade'    then abs(c.mrr_delta) else 0 end as downgrade_mrr,
    case when c.movement_type = 'churn'        then c.mrr_begin_mrr else 0 end as churn_mrr

  from classified c
  left join acct_dim ad
    on ad.account_id = c.account_id
   and c.month_start_date >= (
        case
          when ad.valid_from_month = ad.first_valid_from_month then date '1900-01-01'
          else ad.valid_from_month
        end
      )
   and (ad.valid_to_month is null or c.month_start_date < ad.valid_to_month)

)

select *
from final
order by account_id, month_start_date
