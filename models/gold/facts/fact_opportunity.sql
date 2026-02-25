{{ config(
    incremental_strategy='merge',
    unique_key='sf_opportunity_id'
) }}

with opportunity_base as (
    select
        {{ dbt_utils.generate_surrogate_key(['so.opportunity_id']) }}  as opportunity_key,
        so.opportunity_id   as sf_opportunity_id,
        so.account_id,
        so.owner_user_id,
        so.stage_name,
        so.amount   as amount,
        cast(so.close_date as date)                    as close_date,
        cast(so.silver_load_date as timestamp_ntz)     as base_last_modified_date
    from {{ ref('opportunity') }} so
),

dim_joins as (
    select
        ob.opportunity_key,
        ob.sf_opportunity_id,
        du.dbt_scd_id        as owner_user_key,
        da.dbt_scd_id        as account_key,
        dos.opportunity_stage_key as stage_key,
        ob.amount,
        to_number(to_varchar(ob.close_date, 'YYYYMMDD')) as close_date_key,
        du.dbt_valid_from    as owner_user_changed_at,
        da.dbt_valid_from    as account_changed_at,

        greatest(
          ob.base_last_modified_date,
          du.dbt_valid_from,
          da.dbt_valid_from
        ) as change_ts
    from opportunity_base ob
    left join {{ ref('dim_user') }} du
      on ob.owner_user_id = du.sf_user_id
     and du.dbt_valid_to is null
    left join {{ ref('dim_account') }} da
      on ob.account_id = da.sf_account_id
     and da.dbt_valid_to is null
    left join {{ ref('dim_opportunity_stage') }} dos
      on ob.stage_name = dos.stage_name
),

final as (
    select
        -- PKs
        opportunity_key,
        sf_opportunity_id,
        -- FKs
        account_key,
        owner_user_key,
        stage_key,
        -- Measures / keys
        amount,
        close_date_key,
        -- model’s target timestamp
        change_ts  as last_modified_date
    from dim_joins

    {% if is_incremental() %}
      where change_ts > (
        select coalesce(max(last_modified_date), to_timestamp_ntz('1900-01-01'))
        from {{ this }}
      )
    {% endif %}
)

select *
from final