{{ config(
    incremental_strategy='merge',
    unique_key='sf_opportunity_id'
) }}

WITH opportunity_base AS (
    SELECT
        {{ dbt_utils.surrogate_key(['so.opportunity_id']) }}  AS opportunity_key,
        so.opportunity_id   AS sf_opportunity_id,
        so.account_id,
        so.owner_user_id,
        so.stage_name,
        so.amount   AS amount,
        cast(so.close_date AS date)  AS close_date,
        cast(so.silver_load_date AS timestamp_ntz) AS base_last_modified_date
    FROM {{ ref('opportunity') }} so
),

dim_joins AS (
    SELECT
        ob.opportunity_key,
        ob.sf_opportunity_id,
        du.dbt_scd_id  AS owner_user_key,
        da.dbt_scd_id  AS account_key,
        dos.opportunity_stage_key  AS stage_key,
        ob.amount,
        to_number(to_varchar(ob.close_date, 'YYYYMMDD')) AS close_date_key,
        du.dbt_valid_from  AS owner_user_changed_at,
        da.dbt_valid_from  AS account_changed_at,
           
        greatest(
          ob.base_last_modified_date,
          du.dbt_valid_from,
          da.dbt_valid_from
        )                   AS change_ts
    FROM opportunity_base ob
    LEFT JOIN {{ ref('dim_user') }} du
      on ob.owner_user_id = du.sf_user_id
     AND du.dbt_valid_to is null
    LEFT JOIN {{ ref('dim_account') }} da
      on ob.account_id = da.sf_account_id
     AND da.dbt_valid_to is null
    LEFT JOIN {{ ref('dim_opportunity_stage') }} dos
      on ob.stage_name = dos.stage_name
),

final AS (
    SELECT
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
        change_ts  AS last_modified_date
    FROM dim_joins
    
    {% if is_incremental() %}
      WHERE change_ts > (
        SELECT coalesce(max(silver_load_date), to_timestamp_ntz('1900-01-01'))
        FROM {{ this }}
       )
    {% endif %}

)

SELECT *
FROM final


