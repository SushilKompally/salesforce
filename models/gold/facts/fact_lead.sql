
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='SF_LEAD_ID',
    on_schema_change='append_new_columns',
    contract={'enforced': True}
) }}

WITH base AS (
  SELECT 
      -- PRIMARY KEY (surrogate)
      {{ dbt_utils.surrogate_key(['sl.lead_id']) }}        AS LEAD_ID,

      -- Natural key
      sl.lead_id                                           AS SF_LEAD_ID,

      -- Date key
      TO_NUMBER(TO_CHAR(CAST(sl.created_date AS DATE), 'YYYYMMDD')) AS LEAD_DATE_KEY,

      -- FKs
      du.dbt_scd_id                                        AS OWNER_USER_KEY,
      fo.sf_opportunity_id                                 AS CONVERTED_OPPORTUNITY_KEY,

      -- Details
      sl.company                                           AS COMPANY,
      sl.status                                            AS STATUS,

      -- Audit
      CAST(sl.last_modified_date AS TIMESTAMP_NTZ)         AS LAST_MODIFIED_DATE
  FROM {{ ref('lead') }} sl

  LEFT JOIN {{ ref('dim_dates') }} dd 
    ON TO_NUMBER(TO_CHAR(CAST(sl.created_date AS DATE), 'YYYYMMDD')) = dd.date_key

  LEFT JOIN {{ ref('dim_user') }} du 
    ON sl.owner_user_id = du.sf_user_id 
   AND du.dbt_valid_to IS NULL

  LEFT JOIN {{ ref('fact_opportunity') }} fo
    ON sl.converted_opportunity_id = fo.sf_opportunity_id
),

final AS (
  SELECT
      -- ===========================
      -- PRIMARY KEY
      -- ===========================
      LEAD_ID,
      SF_LEAD_ID,

      -- ===========================
      -- FOREIGN KEYS
      -- ===========================
      OWNER_USER_KEY,
      CONVERTED_OPPORTUNITY_KEY,

      -- ===========================
      -- DATES / KEYS
      -- ===========================
      LEAD_DATE_KEY,

      -- ===========================
      -- DETAILS
      -- ===========================
      COMPANY,
      STATUS,

      -- ===========================
      -- AUDIT / MODEL TARGET TIMESTAMP
      -- ===========================
      LAST_MODIFIED_DATE
  FROM base

  {% if is_incremental() %}
    WHERE LAST_MODIFIED_DATE > (
      SELECT COALESCE(MAX(LAST_MODIFIED_DATE), TO_TIMESTAMP_NTZ('1900-01-01'))
      FROM {{ this }}
    )
  {% endif %}
)

SELECT *
FROM final
