{{ config(materialized='table') }}

with s as (
  select
    account_id,
    account_name,
    industry,
    country,
    signup_date,
    referral_source,
    signup_plan_tier,
    signup_seats,
    signup_is_trial,
    churn_flag,
    ingested_at,
    source_file,
    record_hash,
    dbt_valid_from,
    dbt_valid_to
  from {{ ref('snap_accounts') }}
),

final as (
  select
    -- Deterministic SCD2 surrogate key (natural key + version start)
    md5(concat_ws('|', account_id, cast(dbt_valid_from as varchar))) as account_key,

    -- natural key (degenerate / traceability)
    account_id,

    -- attributes
    account_name,
    industry,
    country,
    signup_date,
    referral_source,
    signup_plan_tier,
    signup_seats,
    signup_is_trial,
    churn_flag,

    -- SCD2 validity
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to,
    (dbt_valid_to is null) as is_current,

    -- lineage / audit
    ingested_at,
    source_file,
    record_hash

  from s
)

select * from final
