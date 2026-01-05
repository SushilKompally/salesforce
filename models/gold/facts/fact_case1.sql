{{ config(
    database='SALESFORCE_DB',
    schema='TEST_GOLD',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='SF_CASE_ID', 
) }}



SELECT
    {{ dbt_utils.surrogate_key(['sc.CASE_ID']) }} AS CASE_ID,
    sc.CASE_ID AS SF_CASE_ID, 
    da.SF_ACCOUNT_ID AS ACCOUNT_ID,        
    dc.SF_CONTACT_ID AS CONTACT_ID,         
    du.SF_USER_ID AS OWNER_USER_ID, 
    dcs.CASE_STATUS_KEY,
    dd1.date_key AS CREATED_DATE_KEY,  
    dd2.date_key AS CLOSED_DATE_KEY, 
    sc.last_modified_date,
FROM {{ ref('case') }} sc
LEFT JOIN {{ ref('dim_account') }} da 
    ON sc.account_id = da.sf_account_id
    AND da.dbt_valid_to IS NULL
LEFT JOIN {{ ref('dim_contact') }} dc 
    ON sc.contact_id = dc.sf_contact_id 
    AND dc.dbt_valid_to IS NULL
LEFT JOIN {{ ref('dim_user') }} du 
    ON sc.owner_user_id = du.sf_user_id 
    AND du.dbt_valid_to IS NULL
LEFT JOIN {{ ref('dim_case_status') }} dcs 
    ON sc.status = dcs.status_name
LEFT JOIN {{ ref('dim_dates') }} dd1 
    ON TO_NUMBER(TO_VARCHAR(CAST(sc.created_date AS DATE), 'YYYYMMDD')) = dd1.date_key
LEFT JOIN {{ ref('dim_dates') }} dd2 
    ON TO_NUMBER(TO_VARCHAR(CAST(sc.closed_date AS DATE), 'YYYYMMDD')) = dd2.date_key

{% if is_incremental() %}
    -- Filter to only new or updated records
    WHERE sc.last_modified_date > (SELECT MAX(last_modified_date) FROM {{ this }})
{% endif %}
