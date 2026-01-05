
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='SF_ACTIVITY_ID',
    on_schema_change='sync_all_columns'
) }}

WITH activity_base AS (
    SELECT
        -- PRIMARY KEY (surrogate)
        {{ dbt_utils.surrogate_key(['e.activity_id']) }} AS activity_key,

        -- Natural key
        e.activity_id                                      AS sf_activity_id,

        -- Raw relationship identifiers
        e.owner_user_id,
        e.what_id,
        e.who_id,

        -- Base timestamp from silver
        CAST(e.silver_load_date AS TIMESTAMP_NTZ)          AS base_last_modified_date
    FROM {{ ref('event') }} e
),

dim_joins AS (
    SELECT
        -- Carry through PKs/natural keys
        ab.activity_key,
        ab.sf_activity_id,

        
        du.dbt_scd_id                                      AS owner_user_key,
     
        ab.what_id,
        ab.who_id,

     
        GREATEST(
            ab.base_last_modified_date,
            du.dbt_valid_from
        )                                                  AS change_ts
    FROM activity_base ab
    LEFT JOIN {{ ref('dim_user') }} du
      ON ab.owner_user_id = du.sf_user_id
     AND du.dbt_valid_to IS NULL  -- only current snapshot row
),

final AS (
    SELECT
        -- ===========================
        -- PRIMARY KEY
        -- ===========================
        activity_key,
        sf_activity_id,

        -- ===========================
        -- FOREIGN KEYS
        -- ===========================
        owner_user_key,

        -- ===========================
        -- DETAILS / RELATIONSHIP IDS
        -- ===========================
        what_id,
        who_id,

        -- ===========================
        -- AUDIT / MODEL TARGET TIMESTAMP
        -- ===========================
        change_ts AS last_modified_date
    FROM dim_joins

    {% if is_incremental() %}
      -- Process only rows newer than the latest in target
      WHERE change_ts > (
        SELECT COALESCE(MAX(last_modified_date), TO_TIMESTAMP_NTZ('1900-01-01'))
        FROM {{ this }}
      )
    {% endif %}
)

SELECT *
FROM final


