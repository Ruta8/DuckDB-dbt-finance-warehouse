{{ config(materialized='table') }}

with src as (
  select * from {{ source('raw', 'support_tickets') }}
),

typed as (
  select
    trim(ticket_id) as ticket_id,
    trim(account_id) as account_id,

    cast(submitted_at as timestamp) as submitted_at,
    cast(closed_at as timestamp) as closed_at,

    -- keep raw + cleaned non-negative
    cast(resolution_time_hours as double) as resolution_time_hours_raw,
    case when cast(resolution_time_hours as double) < 0 then null else cast(resolution_time_hours as double) end as resolution_time_hours,

    lower(nullif(trim(priority), '')) as priority,

    cast(first_response_time_minutes as double) as first_response_time_minutes_raw,
    case when cast(first_response_time_minutes as double) < 0 then null else cast(first_response_time_minutes as double) end as first_response_time_minutes,

    cast(satisfaction_score as double) as satisfaction_score,
    cast(escalation_flag as boolean) as escalation_flag,

    -- convenience
    (cast(closed_at as timestamp) is not null) as is_closed,

    ingested_at,
    source_file,

    md5(
      concat_ws('|',
        trim(ticket_id),
        trim(account_id),
        coalesce(cast(cast(submitted_at as timestamp) as varchar), ''),
        coalesce(cast(cast(closed_at as timestamp) as varchar), ''),
        coalesce(cast(cast(resolution_time_hours as double) as varchar), ''),
        coalesce(lower(nullif(trim(priority), '')), ''),
        coalesce(cast(cast(first_response_time_minutes as double) as varchar), ''),
        coalesce(cast(cast(satisfaction_score as double) as varchar), ''),
        coalesce(cast(cast(escalation_flag as boolean) as varchar), '')
      )
    ) as record_hash

  from src
)

select * from typed
