{{
    config(
        unique_key="OPPORTUNITY_HISTORY_ID",
        incremental_strategy="merge",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("salesforce_bronze", "opportunity_history") }}

    {% if is_incremental() %}
        WHERE CAST(LAST_UPDATED AS TIMESTAMP_NTZ) > (
            SELECT DATEADD(
                DAY,
                -1,
                COALESCE(MAX(LAST_UPADTED), '1900-01-01'::TIMESTAMP_NTZ)
            )
            FROM {{ this }}
        )
        AND 1 = 1
    {% else %}
        WHERE 1 = 1
    {% endif %}
    ),

    CLEANED AS (
  SELECT 
      OPPORTUNITY_HISTORY_ID,
      OPPORTUNITY_ID,
      STAGE_NAME,
      AMOUNT,
      PROBABILITY,
      CLOSE_DATE,
      FIELD,
      OLDVALUE,
      NEWVALUE,
      IS_CLOSED,
      IS_WON,
      CREATED_BY_ID,
      CREATED_DATE,          
      LAST_UPDATED,               
      CURRENT_TIMESTAMP()::TIMESTAMP AS SILVER_LOAD_DATE
      FROM RAW
    )

SELECT *
FROM CLEANED
