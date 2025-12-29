{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='SF_LEAD_ID', 
) }}

SELECT 
    {{ dbt_utils.surrogate_key(['sl.lead_id']) }} AS LEAD_ID,
    sl.lead_id AS SF_LEAD_ID,
    TO_NUMBER(TO_CHAR(sl.created_date, 'YYYYMMDD')) AS LEAD_DATE_KEY,
    du.DBT_SCD_ID AS  OWNER_USER_KEY,
    sl.COMPANY,
    sl.STATUS,
    fo.SF_OPPORTUNITY_ID AS CONVERTED_OPPORTUNITY_KEY,
    sl.LAST_MODIFIED_DATE
FROM {{ ref('lead') }} sl
LEFT JOIN {{ ref('dim_dates') }} dd 
    ON TO_NUMBER(TO_CHAR(sl.created_date, 'YYYYMMDD')) = dd.date_key
LEFT JOIN {{ ref('dim_user') }} du 
    ON sl.owner_user_id = du.sf_user_id 
    AND du.dbt_valid_to IS NULL
LEFT JOIN {{ ref('fact_opportunity') }} fo
    ON sl.converted_opportunity_id = fo.sf_opportunity_id
{% if is_incremental() %}
WHERE sl.LAST_MODIFIED_DATE > (SELECT MAX(LAST_MODIFIED_DATE) FROM {{ this }})
{% endif %}