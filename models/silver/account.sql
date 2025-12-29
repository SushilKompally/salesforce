
/*
-- Description: Incremental Load Script for Silver Layer - account Table
-- Script Name: silver_account.sql
-- Created on: 15-dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     This script performs an incremental load from the Bronze layer to the
--     Silver layer for the acount table in the Salesforce data pipeline.
-- Data source version: v62.0
-- Change History:
--     15-dec-2025 - Initial creation - Sushil Kompally
*/



{{ config(
    unique_key='account_id',
    incremental_strategy='merge',
) }}

WITH raw AS (

     SELECT
        *,
        {{ source_metadata(
         ) }}
    FROM {{ source('salesforce_bronze', 'account') }}
    WHERE 1=1
    {{ incremental_filter() 
    }}

),

cleaned AS (

SELECT
    -- PRIMARY KEY
    id AS account_id,

    -- FOREIGN KEYS
    parentid AS parent_account_id,
    ownerid AS owner_user_id,

    -- NUMERIC
    {{ safe_decimal('annualrevenue') }} AS annual_revenue,
   numberofemployees AS number_of_employees,

    -- DETAILS (strings cleaned)
    {{ clean_string('name') }} AS name,
    {{ clean_string('accountnumber') }} AS account_number,
    {{ clean_string('type') }} AS entity_type,
    {{ clean_string('industry') }} AS industry,
    {{ clean_string('rating') }} AS rating,
    {{ clean_string('ownership') }}  AS ownership,
    {{ clean_string('website') }}  AS website,
    {{ clean_string('tickersymbol') }}  AS ticker_symbol,
    {{ clean_string('phone') }}  AS phone,
    {{ clean_string('fax') }}  AS fax,
    {{ clean_string('billingstreet') }}  AS billing_street,
    {{ clean_string('billingcity') }} AS billing_city,
    {{ clean_string('billingstate') }}   AS billing_state,
    {{ clean_string('billingpostalcode') }}  AS billing_postal_code,
    {{ clean_string('billingcountry') }}  AS billing_country,
    {{ clean_string('shippingstreet') }}  AS shipping_street,
    {{ clean_string('shippingcity') }}   AS shipping_city,
    {{ clean_string('shippingstate') }}    AS shipping_state,
    {{ clean_string('shippingpostalcode') }} AS shipping_postal_code,
    {{ clean_string('shippingcountry') }} AS shipping_country,
    {{ clean_string('site') }}  AS site,
    {{ clean_string('description') }}  AS description,

    -- DATES / TIMESTAMPS
    createddate  AS created_date,
    lastmodifieddate AS last_modified_date,

    -- CREATED BY
    createdbyid AS created_by_id,

     -- LOAD / AUDIT
    current_timestamp()::timestamp_ntz AS silver_load_date,

FROM raw

)
SELECT *
FROM cleaned



