
{#
-- Description: Incremental Load Script for Silver Layer - quote table
-- Script Name: quote_silver.sql
-- Created on: 16-Dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load FROM Bronze to Silver for the quote table using macros
--     for metadata, cleanup, and incremental filtering (merge strategy).
-- Change History:
--     16-Dec-2025 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='quote_id',
    incremental_strategy='merge',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}                                  
    FROM {{ source('salesforce_bronze', 'quote') }}
    WHERE 1=1
  {{ incremental_filter() }}  

),


cleaned AS (

    SELECT
        -- PRIMARY KEY
        id AS quote_id,

        -- FOREIGN KEYS
        opportunityid AS opportunity_id,
        accountid     AS account_id,
        ownerid       AS owner_user_id,
        pricebook2id  AS pricebook2_id,

        -- DETAILS
        {{ clean_string('status') }}        AS status,
        {{ clean_string('quotenumber') }}   AS quote_number,
        {{ clean_string('name') }}          AS name,
        expirationdate                      AS expiration_date,

        -- BILLING ADDRESS
        {{ clean_string('billingstreet') }}      AS billing_street,
        {{ clean_string('billingcity') }}        AS billing_city,
        {{ clean_string('billingstate') }}       AS billing_state,
        {{ clean_string('billingpostalcode') }}  AS billing_postal_code,
        {{ clean_string('billingcountry') }}     AS billing_country,

        -- SHIPPING ADDRESS
        {{ clean_string('shippingstreet') }}      AS shipping_street,
        {{ clean_string('shippingcity') }}        AS shipping_city,
        {{ clean_string('shippingstate') }}       AS shipping_state,
        {{ clean_string('shippingpostalcode') }}  AS shipping_postal_code,
        {{ clean_string('shippingcountry') }}     AS shipping_country,

        -- AMOUNTS
        totalamount  AS total_amount,
        subtotal     AS subtotal,
        discount     AS discount,
        grandtotal   AS grand_total,

        -- AUDIT DATES
        createddate      AS created_date,
        lastmodifieddate AS last_modified_date,

        -- LOAD DATE
        current_timestamp()::timestamp_ntz AS silver_load_date

    FROM raw
)

SELECT *
FROM cleaned
