
/*
-- Description: Incremental Load Script for Silver Layer - lead table
-- Script Name: lead_silver.sql
-- Created on: 16-Dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the lead table using
--     reusable macros for metadata, cleanup, and timestamp safety.
-- Change History:
--     16-Dec-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='LEAD_ID',
    incremental_strategy='merge',
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}       
    FROM {{ source('salesforce_bronze', 'lead') }}
    WHERE 1=1
    {{ incremental_filter() 
    }}
   
),

cleaned AS (

SELECT
    -- primary key
    id                                 AS lead_id,

    -- foreign keys
    ownerid                            AS owner_user_id,

    -- details (strings cleaned)
    {{ clean_string('company') }}      AS company,
    {{ clean_string('firstname') }}    AS first_name,
    {{ clean_string('lastname') }}     AS last_name,
    {{ clean_string('salutation') }}   AS salutation,
    {{ clean_string('title') }}        AS title,
    {{ clean_string('email') }}        AS email,
    {{ clean_string('phone') }}        AS phone,
    {{ clean_string('mobilephone') }}  AS mobile_phone,
    {{ clean_string('website') }}      AS website,
    {{ clean_string('leadsource') }}   AS lead_source,
    {{ clean_string('status') }}       AS status,
    {{ clean_string('rating') }}       AS rating,
    {{ clean_string('industry') }}     AS industry,

    -- numeric (safe conversions when applicable)
    annualrevenue        AS annual_revenue,
    numberofemployees    AS number_of_employees,

    -- address (strings cleaned)
    {{ clean_string('street') }}       AS street,
    {{ clean_string('city') }}         AS city,
    {{ clean_string('state') }}        AS state,
    {{ clean_string('postalcode') }}   AS postal_code,
    {{ clean_string('country') }}      AS country,

    -- conversion fields
    converteddate        AS converted_date,
    convertedaccountid   AS converted_account_id,
    convertedcontactid   AS converted_contact_id,
    convertedopportunityid AS converted_opportunity_id,
    isconverted          AS is_converted,

    -- dates / audit
    createddate          AS created_date,
    createdbyid          AS created_by_id,
    lastmodifieddate     AS last_modified_date,
    lastmodifiedbyid     AS last_modified_by_id,

    -- description (cleaned)
    {{ clean_string('description') }}  AS description,

    -- load metadata
    current_timestamp()::timestamp_ntz AS silver_load_date
FROM raw

)

SELECT *
FROM CLEANED
