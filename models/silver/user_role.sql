
{#
-- Description: Incremental Load Script for Silver userrole - table
-- Script Name: userrole_silver.sql
-- Created on: 16-Dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load FROM Bronze to Silver for the userrole table using macros
--     for metadata, cleanup, and incremental filtering (merge strategy).
-- Change History:
--     16-Dec-2025 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='user_role_id',
    incremental_strategy='merge',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}                                  
    FROM {{ source('salesforce_bronze', 'userrole') }}
    WHERE 1=1
  {{ incremental_filter() }}  

),


cleaned AS (

    SELECT
        -- PRIMARY KEY
        id AS user_role_id,

        -- DETAILS
        {{ clean_string('name') }}             AS name,
        {{ clean_string('developername') }}    AS developer_name,
        {{ clean_string('rollupdescription') }} AS rollup_description,

        -- FOREIGN KEYS
        parentroleid    AS parent_role_id,
        businesshoursid AS business_hours_id,
        forecastuserid  AS forecast_user_id,

        -- AUDIT DATES
        createddate      AS created_date,
        lastmodifieddate AS last_modified_date,

        -- LOAD DATE
        current_timestamp()::timestamp_ntz AS silver_load_date

    FROM raw
)

SELECT *
FROM cleaned
