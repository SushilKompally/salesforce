/*
-- Description: Incremental Load Script for Silver Layer - Contact Table
-- Script Name: silver_contact.sql
-- Created on: 16-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the Contact table.
-- Data source version:
-- Change History:
--     16-dec-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key = 'contact_id',
    incremental_strategy = 'merge',
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('salesforce_bronze', 'contact') }}
    WHERE 1 = 1
    {{ incremental_filter() }}

),

cleaned AS (

    SELECT
        -- PRIMARY KEY
        id AS contact_id,

        -- FOREIGN KEYS
        accountid AS account_id,
        ownerid   AS owner_user_id,

        -- PERSONAL DETAILS
        {{ clean_string('firstname') }}   AS first_name,
        {{ clean_string('lastname') }}    AS last_name,
        {{ clean_string('salutation') }}  AS salutation,
        {{ clean_string('title') }}       AS title,
        {{ clean_string('department') }}  AS department,

        -- CONTACT DETAILS
        {{ clean_string('email') }}        AS email,
        {{ clean_string('phone') }}        AS phone,
        {{ clean_string('mobilephone') }} AS mobile_phone,

        -- ADDRESS (MAILING)
        {{ clean_string('mailingstreet') }}      AS mailing_street,
        {{ clean_string('mailingcity') }}        AS mailing_city,
        {{ clean_string('mailingstate') }}       AS mailing_state,
        {{ clean_string('mailingpostalcode') }}  AS mailing_postal_code,
        {{ clean_string('mailingcountry') }}     AS mailing_country,

        -- ADDRESS (OTHER)
        {{ clean_string('otherstreet') }}      AS other_street,
        {{ clean_string('othercity') }}        AS other_city,
        {{ clean_string('otherstate') }}       AS other_state,
        {{ clean_string('otherpostalcode') }}  AS other_postal_code,
        {{ clean_string('othercountry') }}     AS other_country,

        -- SOURCE / METADATA
        {{ clean_string('leadsource') }} AS lead_source,
        lastmodifiedbyid   AS last_modified_by_id,


        -- AUDIT DATES
        createddate        AS created_date,
        createdbyid        AS created_by_id,
        lastmodifieddate   AS last_modified_date,

        -- AUDIT
        current_timestamp()::timestamp_ntz AS silver_load_date

    FROM raw
)

SELECT *
FROM cleaned;
