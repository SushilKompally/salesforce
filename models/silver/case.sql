
/*
-- Description: Incremental Load Script for Silver Layer - case Table
-- Script Name: silver_case.sql
-- Created on: 16-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the case table.
-- Data source version:
-- Change History:
--     16-dec-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='case_id',
    incremental_strategy='merge',
) }}



WITH raw AS (

  SELECT
    *,
    {{ source_metadata() }}  
  FROM {{ source('salesforce_bronze', 'case') }}
  WHERE 1=1
  {{ incremental_filter() }}  

),

cleaned AS (

  SELECT
    -- PRIMARY KEY
    id AS case_id,

    -- FOREIGN KEYS
    accountid AS account_id,
    contactid AS contact_id,
    ownerid AS owner_user_id,

    -- DETAILS (strings cleaned)
    {{ clean_string('status') }}      AS status,
    {{ clean_string('priority') }}    AS priority,
    {{ clean_string('origin') }}      AS origin,
    {{ clean_string('reason') }}      AS reason,
    {{ clean_string('subject') }}     AS subject,
    {{ clean_string('description') }} AS description,

    -- DATES / TIMESTAMPS
    createddate     AS created_date,
    lastmodifieddate AS last_modified_date,
    closeddate      AS closed_date,

    -- FLAGS / METADATA
    isclosed    AS is_closed,
    is_deleted,

    -- AUDIT
    current_timestamp()::timestamp_ntz           AS silver_load_date,

  FROM raw
)

SELECT *
FROM cleaned