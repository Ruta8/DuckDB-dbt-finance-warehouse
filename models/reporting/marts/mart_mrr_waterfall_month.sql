{{ config(materialized='table') }}

with months as (

    select distinct
        month_start_date
    from {{ ref('dim_date') }}
    where is_month_start = true

),

acct as (

    select
        month_start_date,
        account_id,
        mrr_begin_mrr,
        mrr_end_mrr,
        movement_type,
        new_mrr,
        reactivation_mrr,
        upgrade_mrr,
        downgrade_mrr,
        churn_mrr
    from {{ ref('fct_account_month') }}

),

agg as (

    select
        month_start_date,

        sum(mrr_begin_mrr) as begin_mrr,
        sum(mrr_end_mrr) as end_mrr,
        sum(mrr_end_mrr) - sum(mrr_begin_mrr) as net_mrr_change,

        sum(new_mrr) as new_mrr,
        sum(reactivation_mrr) as reactivation_mrr,
        sum(upgrade_mrr) as expansion_mrr,
        sum(downgrade_mrr) as contraction_mrr,
        sum(churn_mrr) as churn_mrr,

        count(distinct case when mrr_end_mrr > 0 then account_id end) as active_accounts,
        count(distinct case when movement_type = 'churn' then account_id end) as churned_accounts,
        count(distinct case when movement_type = 'new' then account_id end) as new_accounts,
        count(distinct case when movement_type = 'reactivation' then account_id end) as reactivated_accounts

    from acct
    group by 1

),

final as (

    select
        m.month_start_date,

        coalesce(a.begin_mrr, 0) as begin_mrr,
        coalesce(a.end_mrr, 0) as end_mrr,
        coalesce(a.net_mrr_change, 0) as net_mrr_change,

        coalesce(a.new_mrr, 0) as new_mrr,
        coalesce(a.reactivation_mrr, 0) as reactivation_mrr,
        coalesce(a.expansion_mrr, 0) as expansion_mrr,
        coalesce(a.contraction_mrr, 0) as contraction_mrr,
        coalesce(a.churn_mrr, 0) as churn_mrr,

        coalesce(a.active_accounts, 0) as active_accounts,
        coalesce(a.churned_accounts, 0) as churned_accounts,
        coalesce(a.new_accounts, 0) as new_accounts,
        coalesce(a.reactivated_accounts, 0) as reactivated_accounts

    from months m
    left join agg a
      on a.month_start_date = m.month_start_date

)

select *
from final
order by month_start_date
