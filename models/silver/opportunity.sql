/*
-- Description: Incremental Load Script for Silver Layer - opportunity Table
-- Script Name: silver_opportunity.sql
-- Created on: 16-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the opportunity table.
-- Data source version:v62.0
-- Change History:
--     16-dec-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='opportunity_id',
    incremental_strategy='merge',
) }}


WITH raw AS (

  SELECT
    *,
    {{ source_metadata() }}
  FROM {{ source('salesforce_bronze', 'opportunity') }}
  WHERE 1=1
  {{ incremental_filter() }}  

),

    
cleaned AS (

    SELECT
        -- PRIMARY KEY
        id AS opportunity_id,

        -- FOREIGN KEYS
        accountid AS account_id,
        ownerid   AS owner_user_id,
        campaignid AS campaign_id,

        -- DETAILS
        {{ clean_string('name') }}                AS name,
        {{ clean_string('stagename') }}           AS stage_name,
        amount                                    AS amount,
        probability                               AS probability,
        closedate                                 AS close_date,
        {{ clean_string('type') }}                AS entity_type,
        {{ clean_string('leadsource') }}          AS lead_source,
        {{ clean_string('forecastcategoryname') }} AS forecast_category_name,

        -- STATUS FLAGS
        isclosed AS is_closed,
        iswon    AS is_won,

        -- ADDITIONAL INFO
        {{ clean_string('nextstep') }}           AS next_step,
        {{ clean_string('primarycompetitor') }}  AS primary_competitor,
        {{ clean_string('description') }}        AS description,

        -- AUDIT DATES
        createddate      AS created_date,
        lastmodifieddate AS last_modified_date,

        -- LOAD DATE
        current_timestamp()::timestamp_ntz AS silver_load_date

    FROM raw
)

SELECT *
FROM cleaned
