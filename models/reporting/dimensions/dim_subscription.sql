{{ config(materialized='table') }}

with s as (
  select
    subscription_id,
    account_id,
    start_date,
    end_date,
    plan_tier,
    seats,
    billing_frequency,
    is_trial,
    upgrade_flag,
    downgrade_flag,
    churn_flag,
    auto_renew_flag,
    mrr_amount,
    arr_amount,
    mrr_amount_raw,
    arr_amount_raw,
    ingested_at,
    source_file,
    record_hash,
    dbt_valid_from,
    dbt_valid_to
  from {{ ref('snap_subscriptions') }}
),

acct as (
  -- we’ll join to account dim to attach account_key for the same valid_from window
  select
    account_id,
    account_key,
    valid_from,
    valid_to
  from {{ ref('dim_account') }}
),

joined as (
  select
    s.*,
    a.account_key
  from s
  left join acct a
    on a.account_id = s.account_id
   and s.dbt_valid_from >= a.valid_from
   and (a.valid_to is null or s.dbt_valid_from < a.valid_to)
),

final as (
  select
    -- Deterministic SCD2 surrogate key (natural key + version start)
    md5(concat_ws('|', subscription_id, cast(dbt_valid_from as varchar))) as subscription_key,

    -- natural keys (degenerate / traceability)
    subscription_id,
    account_id,

    -- FK to account dimension (as-of the subscription version start)
    account_key,

    -- attributes (Type 2)
    start_date,
    end_date,
    plan_tier,
    seats,
    billing_frequency,
    is_trial,
    upgrade_flag,
    downgrade_flag,
    churn_flag,
    auto_renew_flag,

    --  attached to subscription state
    mrr_amount,
    arr_amount,
    mrr_amount_raw,
    arr_amount_raw,

    -- SCD2 validity
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to,
    (dbt_valid_to is null) as is_current,

    -- lineage / audit
    ingested_at,
    source_file,
    record_hash

  from joined
)

select * from final
