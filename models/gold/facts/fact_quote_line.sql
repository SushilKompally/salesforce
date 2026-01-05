
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='SF_QUOTE_LINE_ITEM_ID',
    on_schema_change='append_new_columns',
    contract={'enforced': true}
) }}

WITH base AS (
  SELECT
      {{ dbt_utils.surrogate_key(['sqli.QUOTE_LINE_ITEM_ID']) }} AS QUOTE_LINE_ITEM_KEY,
      sqli.QUOTE_LINE_ITEM_ID                                    AS SF_QUOTE_LINE_ITEM_ID,
      fq.QUOTE_KEY,
      dp.DBT_SCD_ID                                              AS PRODUCT_KEY,
      sqli.QUANTITY,
      sqli.UNIT_PRICE,
      CAST(sqli.LAST_MODIFIED_DATE AS TIMESTAMP_NTZ)             AS LAST_MODIFIED_DATE
  FROM {{ ref('quote_lineitem') }} sqli
  LEFT JOIN {{ ref('fact_quote') }} fq
    ON sqli.quote_id = fq.sf_quote_id
  LEFT JOIN {{ ref('dim_product') }} dp
    ON sqli.product_id = dp.SF_PRODUCT_ID
   AND dp.dbt_valid_to IS NULL
),

final AS (
  SELECT
      -- ===========================
      -- PRIMARY KEY
      -- ===========================
      QUOTE_LINE_ITEM_KEY,
      SF_QUOTE_LINE_ITEM_ID,

      -- ===========================
      -- FOREIGN KEYS
      -- ===========================
      QUOTE_KEY,
      PRODUCT_KEY,

      -- ===========================
      -- MEASURES / DETAILS
      -- ===========================
      QUANTITY,
      UNIT_PRICE,

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
