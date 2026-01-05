/*
-- Description: Incremental Load Script for Silver Layer - campaign Table
-- Script Name: silver_campaign.sql
-- Created on: 16-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the campaign table.
-- Data source version:v62.0
-- Change History:
--     16-dec-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='campaign_id',
    incremental_strategy='merge',
) }}


WITH raw AS (

  SELECT
    *,
    {{ source_metadata() }}
  FROM {{ source('salesforce_bronze', 'campaign') }}
  WHERE 1=1
  {{ incremental_filter() }}  

),

cleaned AS (

  SELECT
    -- PRIMARY KEY
    id AS campaign_id,

    -- FOREIGN KEYS
    ownerid AS owner_user_id,

    -- NUMERIC
    expectedrevenue AS expected_revenue,
    budgetedcost    AS budgeted_cost,
    actualcost      AS actual_cost,
    numbersent      AS number_sent,

    -- DETAILS (strings cleaned)
    {{ clean_string('name') }}     AS name,
    {{ clean_string('type') }}     AS entity_type,
    {{ clean_string('status') }}   AS status,
    {{ clean_string('description') }}  AS description,

    -- DATES / TIMESTAMPS
    {{ safe_date('startdate') }}    AS start_date,
    {{ safe_date('enddate') }}     AS end_date,
    createddate     AS created_date,
    lastmodifieddate AS last_modified_date,

   

    -- LOAD / AUDIT
    current_timestamp()::timestamp_ntz AS silver_load_date,

  FROM raw
)

SELECT *
FROM
cleaned