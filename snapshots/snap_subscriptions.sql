{% snapshot snap_subscriptions %}

{{
  config(
    target_schema='snapshots',
    unique_key='subscription_id',
    strategy='check',
    check_cols=['record_hash'],
    invalidate_hard_deletes=true
  )
}}

select
  -- natural key
  subscription_id,

  -- relationship
  account_id,

  -- business attributes
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

  -- attributable to subscription state
  mrr_amount,
  arr_amount,
  mrr_amount_raw,
  arr_amount_raw,

  -- metadata
  ingested_at,
  source_file,

  -- change detection
  record_hash

from {{ ref('int_subscriptions_current') }}

{% endsnapshot %}
