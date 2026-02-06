
{{ config(
    database='SALESFORCE_DB',
    schema='TEST_GOLD',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='SF_ACTIVITY_ID',
    on_schema_change='append_new_columns',
) }}

SELECT    
    {{ dbt_utils.surrogate_key(['e.activity_id']) }} AS activity_key,
    e.activity_id         AS sf_activity_id,
    du.sf_user_id         AS owner_user_id,
    e.what_id,
    e.who_id,
    e.silver_load_date
FROM {{ ref('event') }} e
LEFT JOIN {{ ref('dim_user') }} du
  ON e.owner_user_id = du.sf_user_id
  AND du.dbt_valid_to IS NULL  -- only current snapshot row

{% if is_incremental() %}
WHERE CAST(e.silver_load_date AS TIMESTAMP_NTZ) > (
  SELECT COALESCE(MAX(silver_load_date), '1900-01-01'::TIMESTAMP_NTZ)
  FROM {{ this }}
)
{% endif %}
