
{{ config(
    materialized='table',
    database='SALESFORCE_DB',
    schema='TEST_GOLD'
) }}

WITH dates AS (
    SELECT
        DATEADD(day, SEQ4(), DATE '2015-01-01') AS calendar_date
    FROM TABLE(GENERATOR(ROWCOUNT => 10000))  -- 5844 days between 2015-01-01 and 2030-12-31 inclusive
)

SELECT
    TO_NUMBER(TO_CHAR(calendar_date, 'YYYYMMDD')) AS date_key,
    calendar_date,
    EXTRACT(year FROM calendar_date) AS year,
    EXTRACT(month FROM calendar_date) AS month,
    TO_CHAR(calendar_date, 'Month') AS month_name,
    CEIL(EXTRACT(month FROM calendar_date) / 3) AS quarter,
    EXTRACT(dow FROM calendar_date) + 1 AS day_of_week,
    TO_CHAR(calendar_date, 'Day') AS day_name,
    WEEK(calendar_date) AS week_of_year
FROM dates
ORDER BY calendar_date