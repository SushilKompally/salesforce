
{{ config(
    database='SALESFORCE_DB',
    schema='TEST_GOLD',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='SF_ACTIVITY_ID',
) }}


SELECT    
    {{ dbt_utils.surrogate_key(['e.activity_id']) }} AS ACTIVITY_KEY,
    e.activity_id AS SF_ACTIVITY_ID,
    du.SF_USER_ID AS  OWNER_USER_ID,
    e.WHAT_ID,
    e.WHO_ID
FROM {{ ref('event') }} e
LEFT JOIN  {{ ref('dim_user') }} du
    ON e.owner_user_id = du.sf_user_id
    AND du.dbt_valid_to IS NULL

 {% if is_incremental() %}
    WHERE CAST(e.silver_load_date AS TIMESTAMP_NTZ) > (
        SELECT COALESCE(MAX(LAST_MODIFIED_DATE), '1900-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
    )
    {% endif %}
