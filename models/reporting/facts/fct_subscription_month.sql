{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['subscription_id','month_start_date']
) }}

with months as (
  select distinct
    month_start_date,
    next_month_start_date
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

month_bounds as (
  select
    min(month_start_date) as min_month,
    max(month_start_date) as max_month
  from months
),

subs as (
  select
    subscription_id,
    account_id,
    cast(start_date as date) as start_date,
    cast(end_date as date) as end_date,
    is_trial,
    mrr_amount,
    arr_amount
  from {{ ref('stg_subscriptions') }}
),

bounded as (
  select
    s.*,
    date_trunc('month', s.start_date)::date as start_month,
    least(
      date_trunc('month', coalesce(s.end_date, (select max_month from month_bounds)))::date,
      (select max_month from month_bounds)
    ) as end_month
  from subs s
),


spine as (
  select
    b.*,
    m.month_start_date,
    m.next_month_start_date
  from bounded b
  join months m
    on m.month_start_date between b.start_month and b.end_month
),

base_fact as (
  select
    month_start_date,
    subscription_id,
    account_id,

    start_date as subscription_start_date,
    end_date as subscription_end_date,
    
--active on the last day of the month
  (
    start_date < next_month_start_date
    and (end_date is null or end_date >= (next_month_start_date - interval 1 day))
  ) as is_active_at_eom,

    (date_trunc('month', start_date)::date = month_start_date) as is_start_month,
    (end_date is not null and date_trunc('month', end_date)::date = month_start_date) as is_end_month,

  case
    when (
      start_date < next_month_start_date
      and (end_date is null or end_date >= (next_month_start_date - interval 1 day))
    )
    and coalesce(is_trial, false) = false
    then coalesce(mrr_amount, 0)
    else 0
  end as mrr_amount

  from spine
),

-- month-grain SCD2 validity + first version is valid forever back
sub_dim as (
  select
    subscription_id,
    subscription_key,
    account_key,

    date_trunc('month', valid_from)::date as valid_from_month,
    case when valid_to is null then null else date_trunc('month', valid_to)::date end as valid_to_month,

    min(date_trunc('month', valid_from)::date) over (partition by subscription_id) as first_valid_from_month
  from {{ ref('dim_subscription') }}
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

with_sub_key as (
  select
    f.*,
    sd.subscription_key,
    sd.account_key as subscription_account_key
  from base_fact f
  left join sub_dim sd
    on sd.subscription_id = f.subscription_id
   and f.month_start_date >= (
        case
          when sd.valid_from_month = sd.first_valid_from_month then date '1900-01-01'
          else sd.valid_from_month
        end
      )
   and (sd.valid_to_month is null or f.month_start_date < sd.valid_to_month)
),

final as (
  select
    -- surrogate FKs for BI
    w.subscription_key,
    coalesce(w.subscription_account_key, ad.account_key) as account_key,

    -- degenerate ids
    w.subscription_id,
    w.account_id,

    w.month_start_date,

    w.mrr_amount,
    w.is_active_at_eom,
    w.is_start_month,
    w.is_end_month,

    w.subscription_start_date,
    w.subscription_end_date

  from with_sub_key w
  left join acct_dim ad
    on ad.account_id = w.account_id
   and w.month_start_date >= (
        case
          when ad.valid_from_month = ad.first_valid_from_month then date '1900-01-01'
          else ad.valid_from_month
        end
      )
   and (ad.valid_to_month is null or w.month_start_date < ad.valid_to_month)
)

select *
from final
order by subscription_id, month_start_date
