{{
    config(
        materialized="table",
    )
}}

WITH aggregated_data AS (
    SELECT
        STAGE_NAME,
        MAX(PROBABILITY) AS PROBABILITY,
        MAX(IS_CLOSED) AS IS_CLOSED
   FROM {{ ref('opportunity') }}
    WHERE STAGE_NAME IS NOT NULL
    GROUP BY STAGE_NAME
)

SELECT
    {{ dbt_utils.surrogate_key(['STAGE_NAME']) }} AS OPPORTUNITY_STAGE_KEY,
    STAGE_NAME,
    PROBABILITY,
    IS_CLOSED
FROM aggregated_data