
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='SF_QUOTE_ID',   
    on_schema_change='append_new_columns',
    contract={'enforced': true}
 
) }}

WITH base AS (
  SELECT 
      {{ dbt_utils.surrogate_key(['sq.quote_id']) }} AS QUOTE_KEY,
      sq.quote_id                                     AS SF_QUOTE_ID,
      fo.OPPORTUNITY_KEY,
      da.DBT_SCD_ID                                   AS ACCOUNT_KEY,
      dd.date_key                                     AS CREATED_DATE_KEY,
      CAST(sq.LAST_MODIFIED_DATE AS TIMESTAMP_NTZ)    AS LAST_MODIFIED_DATE
  FROM {{ ref('quote') }} sq
  LEFT JOIN {{ ref('fact_opportunity') }} fo 
    ON sq.opportunity_id = fo.sf_opportunity_id
  LEFT JOIN {{ ref('dim_account') }} da 
    ON sq.account_id = da.sf_account_id 
   AND da.dbt_valid_to IS NULL
  LEFT JOIN {{ ref('dim_dates') }} dd 
    ON TO_NUMBER(TO_VARCHAR(CAST(sq.created_date AS DATE),'YYYYMMDD')) = dd.date_key
),

final AS (
  SELECT
      -- ===========================
      -- PRIMARY KEY
      -- ===========================
      QUOTE_KEY,
      SF_QUOTE_ID,

      -- ===========================
      -- FOREIGN KEYS
      -- ===========================
      OPPORTUNITY_KEY,
      ACCOUNT_KEY,

      -- ===========================
      -- DATES / KEYS
      -- ===========================
      CREATED_DATE_KEY,

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
