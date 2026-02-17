{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

with src as (
  select *
  from {{ source('raw', 'accounts') }}
  {% if is_incremental() %}
    where ingested_at > (select max(ingested_at) from {{ this }})
  {% endif %}
),

typed as (
  select
    trim(account_id) as account_id,
    nullif(trim(account_name), '') as account_name,
    nullif(trim(industry), '') as industry,
    nullif(trim(country), '') as country,

    cast(signup_date as date) as signup_date,
    nullif(trim(referral_source), '') as referral_source,

    nullif(trim(plan_tier), '') as signup_plan_tier,
    cast(seats as integer) as signup_seats,
    cast(is_trial as boolean) as signup_is_trial,

    cast(churn_flag as boolean) as churn_flag,

    ingested_at,
    source_file,

    md5(
      concat_ws('|',
        trim(account_id),
        coalesce(nullif(trim(account_name), ''), ''),
        coalesce(nullif(trim(industry), ''), ''),
        coalesce(nullif(trim(country), ''), ''),
        coalesce(cast(cast(signup_date as date) as varchar), ''),
        coalesce(nullif(trim(referral_source), ''), ''),
        coalesce(nullif(trim(plan_tier), ''), ''),
        coalesce(cast(cast(seats as integer) as varchar), ''),
        coalesce(cast(cast(is_trial as boolean) as varchar), ''),
        coalesce(cast(cast(churn_flag as boolean) as varchar), '')
      )
    ) as record_hash

  from src
)

select * from typed
