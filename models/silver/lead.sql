
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
        {{ source_metadata() }}       -- adds ingestion_tool, source_last_modified, etc.
    FROM {{ source('salesforce_bronze', 'lead') }}
    WHERE 1=1
    {{ incremental_filter() 
    }}
   
),

cleaned AS (

    SELECT
        -- PRIMARY KEY
        id                                 AS LEAD_ID,

        -- FOREIGN KEYS
        ownerid                            AS OWNER_USER_ID,

        -- DETAILS (strings cleaned)
        {{ clean_string('company') }}      AS COMPANY,
        {{ clean_string('firstname') }}    AS FIRST_NAME,
        {{ clean_string('lastname') }}     AS LAST_NAME,
        {{ clean_string('salutation') }}   AS SALUTATION,
        {{ clean_string('title') }}        AS TITLE,
        {{ clean_string('email') }}        AS EMAIL,
        {{ clean_string('phone') }}        AS PHONE,
        {{ clean_string('mobilephone') }}  AS MOBILE_PHONE,
        {{ clean_string('website') }}      AS WEBSITE,
        {{ clean_string('leadsource') }}   AS LEAD_SOURCE,
        {{ clean_string('status') }}       AS STATUS,
        {{ clean_string('rating') }}       AS RATING,
        {{ clean_string('industry') }}     AS INDUSTRY,

        -- NUMERIC (safe conversions when applicable)
        annualrevenue        AS ANNUAL_REVENUE,
        numberofemployees   AS NUMBER_OF_EMPLOYEES,

        -- ADDRESS (strings cleaned)
        {{ clean_string('street') }}       AS STREET,
        {{ clean_string('city') }}         AS CITY,
        {{ clean_string('state') }}        AS STATE,
        {{ clean_string('postalcode') }}   AS POSTAL_CODE,
        {{ clean_string('country') }}      AS COUNTRY,

        -- CONVERSION FIELDS
        converteddate       AS CONVERTED_DATE,
        convertedaccountid                           AS CONVERTED_ACCOUNT_ID,
        convertedcontactid                           AS CONVERTED_CONTACT_ID,
        convertedopportunityid                       AS CONVERTED_OPPORTUNITY_ID,
        isconverted                                  AS IS_CONVERTED,

        -- DATES / AUDIT
        createddate          AS CREATED_DATE,
        createdbyid                                  AS CREATED_BY_ID,
        lastmodifieddate    AS LAST_MODIFIED_DATE,
        lastmodifiedbyid                             AS LAST_MODIFIED_BY_ID,

        -- DESCRIPTION (cleaned)
        {{ clean_string('description') }}            AS DESCRIPTION,

        -- LOAD METADATA
        current_timestamp()::timestamp_ntz           AS SILVER_LOAD_DATE

       FROM raw
)

SELECT *
FROM CLEANED
