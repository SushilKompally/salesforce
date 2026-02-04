
{#
-- Description: Incremental Load Script for Silver Layer - user table
-- Script Name: user_silver.sql
-- Created on: 16-Dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load FROM Bronze to Silver for the user table using macros
--     for metadata, cleanup, and incremental filtering (merge strategy).
-- Change History:
--     16-Dec-2025 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='user_id',
    incremental_strategy='merge',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}                                  
    FROM {{ source('salesforce_bronze', 'user') }}
    WHERE 1=1
  {{ incremental_filter() }}  

),


cleaned AS (

    SELECT
        -- PRIMARY KEY
        id AS user_id,

        -- LOGIN DETAILS
        {{ clean_string('username') }} AS username,
        {{ clean_string('email') }}    AS email,
        {{ clean_string('alias') }}    AS alias,

        -- PERSONAL DETAILS
        {{ clean_string('firstname') }} AS first_name,
        {{ clean_string('lastname') }}  AS last_name,
        isactive                        AS is_active,

        -- ROLE & PROFILE
        userroleid AS user_role_id,
        profileid  AS profile_id,

        -- JOB INFO
        {{ clean_string('title') }}      AS title,
        {{ clean_string('department') }} AS department,
        managerid                        AS manager_id,

        -- AUDIT DATES
        createddate       AS created_date,
        lastlogindate     AS last_login_date,
        lastmodifieddate  AS last_modified_date,

        -- LOCALE SETTINGS
        {{ clean_string('timezonesidkey') }}      AS time_zone_sid_key,
        {{ clean_string('localesidkey') }}        AS locale_sid_key,
        {{ clean_string('languagelocalekey') }}   AS language_locale_key,
       --- isdeleted  AS is_deleted,

        -- LOAD DATE
        current_timestamp()::timestamp_ntz AS silver_load_date

    FROM raw
)

SELECT *
FROM cleaned
