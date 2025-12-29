{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='SF_QUOTE_LINE_ITEM_ID', 
) }}

SELECT
    {{ dbt_utils.surrogate_key(['sqli.QUOTE_LINE_ITEM_ID']) }} AS QUOTE_LINE_ITEM_KEY,
    sqli.QUOTE_LINE_ITEM_ID AS SF_QUOTE_LINE_ITEM_ID,
    fq.QUOTE_KEY,
    dp.DBT_SCD_ID AS PRODUCT_KEY,
    sqli.QUANTITY,
    sqli.UNIT_PRICE,
    sqli.LAST_MODIFIED_DATE
FROM {{ ref('quote_lineitem') }} sqli
LEFT JOIN {{ ref('fact_quote') }} fq
    ON sqli.quote_id = fq.sf_quote_id
LEFT JOIN {{ ref('dim_product') }} dp
    ON sqli.product_id = dp.SF_PRODUCT_ID
    AND dp.dbt_valid_to is null

{% if is_incremental() %}
    WHERE sqli.LAST_MODIFIED_DATE > (SELECT MAX(LAST_MODIFIED_DATE) FROM {{ this }})  -- Filter for new or updated rows
{% endif %}