{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='SF_QUOTE_ID', 
) }}

SELECT 
    {{ dbt_utils.surrogate_key(['sq.quote_id']) }} AS QUOTE_KEY,
    sq.quote_id AS SF_QUOTE_ID,
    fo.OPPORTUNITY_KEY,
    da.DBT_SCD_ID AS ACCOUNT_KEY,
    dd.date_key AS CREATED_DATE_KEY,
    sq.LAST_MODIFIED_DATE
FROM {{ ref('quote') }} sq
LEFT JOIN {{ ref('fact_opportunity') }} fo 
    ON sq.opportunity_id = fo.sf_opportunity_id
LEFT JOIN {{ ref('dim_account') }} da 
    ON sq.account_id = da.sf_account_id 
    AND da.dbt_valid_to IS NULL
LEFT JOIN {{ ref('dim_dates') }} dd 
    ON TO_NUMBER(TO_VARCHAR(CAST(sq.created_date AS DATE),'YYYYMMDD')) = dd.date_key

{% if is_incremental() %}
    WHERE sq.LAST_MODIFIED_DATE > (SELECT MAX(LAST_MODIFIED_DATE) FROM {{ this }})  -- Filtering new or updated records
{% endif %}