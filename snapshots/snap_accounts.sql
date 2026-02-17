{% snapshot snap_accounts %}

{{
  config(
    target_schema='snapshots',
    unique_key='account_id',
    strategy='check',
    check_cols=['record_hash'],
    invalidate_hard_deletes=true
  )
}}

select
  -- natural key
  account_id,

  -- business attributes
  account_name,
  industry,
  country,
  signup_date,
  referral_source,
  signup_plan_tier,
  signup_seats,
  signup_is_trial,
  churn_flag,

  -- metadata (kept for audit / lineage)
  ingested_at,
  source_file,

  -- change detection
  record_hash

from {{ ref('int_accounts_current') }}

{% endsnapshot %}
