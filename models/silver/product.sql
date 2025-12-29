
/*
-- Description: Incremental Load Script for Silver Layer - product table
-- Script Name: event_silver.sql
-- Created on: 16-Dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load FROM Bronze to Silver for the product table using macros
--     for metadata, cleanup, and incremental filtering (merge strategy).
-- Change History:
--     16-Dec-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='product_id',
    incremental_strategy='merge',
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}                                  
    FROM {{ source('salesforce_bronze', 'product') }}
    WHERE 1=1
  {{ incremental_filter() }}  

),

cleaned AS (

    SELECT
        -- PRIMARY KEY
        id                                 AS activity_id,

        -- FOREIGN KEYS
        ownerid                            AS owner_user_id,
        whoid                              AS who_id,
        whatid                             AS what_id,

        -- DETAILS (strings cleaned)
        {{ clean_string('subject') }}      AS subject,
        {{ clean_string('description') }}  AS description,

        -- DATES / TIMESTAMPS (Snowflake-safe)
        createddate      AS created_date,
        lastmodifieddate AS last_modified_date,
        activitydate      AS activity_date,

        -- FLAGS / METADATA
        isdeleted                         AS is_deleted,
        current_timestamp()::timestamp_ntz AS silver_load_date
    FROM raw
)

SELECT *
FROM cleaned

