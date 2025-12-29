{{
    config(
        materialized="table",
    )
}}

WITH distinct_cases AS (
    SELECT DISTINCT
        case_id,
        status,
        is_closed
    FROM {{ ref('case') }}
)

SELECT 
    {{ dbt_utils.surrogate_key(['status', 'is_closed']) }} AS case_status_key,
    status AS status_name,
    is_closed
FROM distinct_cases