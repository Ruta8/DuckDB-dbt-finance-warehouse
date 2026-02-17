{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

with src as (
  select *
  from {{ source('raw', 'subscriptions') }}
  {% if is_incremental() %}
    where ingested_at > (select max(ingested_at) from {{ this }})
  {% endif %}
),

typed as (
  select
    trim(subscription_id) as subscription_id,
    trim(account_id) as account_id,

    cast(start_date as date) as start_date,
    cast(end_date as date) as end_date,

    nullif(trim(plan_tier), '') as plan_tier,
    cast(seats as integer) as seats,

    cast(mrr_amount as double) as mrr_amount_raw,
    cast(arr_amount as double) as arr_amount_raw,
    case when cast(mrr_amount as double) < 0 then null else cast(mrr_amount as double) end as mrr_amount,
    case when cast(arr_amount as double) < 0 then null else cast(arr_amount as double) end as arr_amount,

    cast(is_trial as boolean) as is_trial,
    cast(upgrade_flag as boolean) as upgrade_flag,
    cast(downgrade_flag as boolean) as downgrade_flag,
    cast(churn_flag as boolean) as churn_flag,

    lower(nullif(trim(billing_frequency), '')) as billing_frequency,
    cast(auto_renew_flag as boolean) as auto_renew_flag,

    ingested_at,
    source_file,

    md5(
      concat_ws('|',
        trim(subscription_id),
        trim(account_id),
        coalesce(cast(cast(start_date as date) as varchar), ''),
        coalesce(cast(cast(end_date as date) as varchar), ''),
        coalesce(nullif(trim(plan_tier), ''), ''),
        coalesce(cast(cast(seats as integer) as varchar), ''),
        coalesce(cast(cast(mrr_amount as double) as varchar), ''),
        coalesce(cast(cast(arr_amount as double) as varchar), ''),
        coalesce(cast(cast(is_trial as boolean) as varchar), ''),
        coalesce(cast(cast(upgrade_flag as boolean) as varchar), ''),
        coalesce(cast(cast(downgrade_flag as boolean) as varchar), ''),
        coalesce(cast(cast(churn_flag as boolean) as varchar), ''),
        coalesce(lower(nullif(trim(billing_frequency), '')), ''),
        coalesce(cast(cast(auto_renew_flag as boolean) as varchar), '')
      )
    ) as record_hash

  from src
)

select * from typed
