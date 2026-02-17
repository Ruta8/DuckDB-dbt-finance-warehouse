{{ config(materialized='table') }}

{% set start_date = var('dim_date_start_date', '2022-01-01') %}
{% set end_date   = var('dim_date_end_date',   '2025-12-31') %}
{% set epoch_month = '1970-01-01' %}

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ start_date ~ "' as date)",
        end_date="cast('" ~ end_date ~ "' as date)"
    ) }}

),

base as (

    select
        cast(date_day as date) as date_day,
        date_trunc('month', cast(date_day as date))::date as month_start_date
    from spine

),

final as (

    select
        date_day,
        month_start_date,

        datediff('month', cast('{{ epoch_month }}' as date), month_start_date) as month_index,

        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,

        (extract(year from date_day) * 100 + extract(month from date_day))::int as year_month,

        strftime(date_day, '%B') as month_name,

        (month_start_date + interval '1 month' - interval '1 day')::date as month_end_date,

        (date_day = month_start_date) as is_month_start,
        (date_day = (month_start_date + interval '1 month' - interval '1 day')::date) as is_month_end,

        (month_start_date - interval '1 month')::date as prev_month_start_date,
        (month_start_date + interval '1 month')::date as next_month_start_date

    from base
)

select * from final
order by date_day
