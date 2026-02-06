
{#
-- Description: Incremental Load Script for Silver Layer - opportunity_history Table
-- Script Name: opportunity_history.sql
-- Created on: 16-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the opportunity_history table.
-- Data source version:v62.0
-- Change History:
--     16-dec-2025 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='opportunity_history_id',
    incremental_strategy='merge',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}


WITH raw AS (

  SELECT
    *,
    {{ source_metadata() }}
   FROM {{ source('salesforce_bronze', 'opportunity_history') }}
  WHERE 1=1
  {{ incremental_filter() }}  

),



cleaned AS (

    SELECT
        -- PRIMARY KEY
        opportunity_history_id AS opportunity_history_id,

        -- FOREIGN KEYS
        opportunity_id AS opportunity_id,

        -- DETAILS
        {{ clean_string('stage_name') }} AS stage_name,
        amount                          AS amount,
        probability                     AS probability,
        close_date                      AS close_date,

        -- CHANGE TRACKING
        {{ clean_string('field') }}     AS field,
        {{ clean_string('oldvalue') }}  AS oldvalue,
        {{ clean_string('newvalue') }}  AS newvalue,

        -- STATUS FLAGS
        is_closed AS is_closed,
        is_won    AS is_won,

        -- AUDIT
        created_by_id      AS created_by_id,
        created_date       AS created_date,
        last_updated       AS last_updated,

        -- LOAD DATE
        current_timestamp()::timestamp_ntz AS silver_load_date

    FROM raw
)

SELECT *
FROM cleaned
