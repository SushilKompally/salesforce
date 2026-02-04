
{#
-- Description: Incremental Load Script for Silver Layer - product table
-- Script Name: event_silver.sql
-- Created on: 16-Dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load FROM Bronze to Silver for the product table using macros
--     for metadata, cleanup, and incremental filtering (merge strategy).
-- Change History:
--     16-Dec-2025 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='product_id',
    incremental_strategy='merge',
    pre_hook = "{{ log_model_audit(status='STARTED') }}",
    post_hook = "{{ log_model_audit(status='SUCCESS') }}"
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}                                  
    FROM {{ source('salesforce_bronze', 'product') }}
    WHERE 1=1
  {{ incremental_filter() }}  

),


cleaned AS (

    SELECT
        -- PRIMARY KEY
        id AS product_id,

        -- DETAILS
        {{ clean_string('name') }}                  AS name,
        {{ clean_string('productcode') }}          AS product_code,
        {{ clean_string('description') }}          AS description,
        {{ clean_string('family') }}               AS family,
        isactive                                   AS is_active,

        -- ADDITIONAL INFO
        {{ clean_string('quantityunitofmeasure') }} AS quantity_unit_of_measure,
        {{ clean_string('vendorproductcode') }}     AS vendor_product_code,
        {{ clean_string('manufacturer') }}          AS manufacturer,

        -- AUDIT DATES
        createddate      AS created_date,
        lastmodifieddate AS last_modified_date,

        -- LOAD DATE
        current_timestamp()::timestamp_ntz AS silver_load_date

    FROM raw
)

SELECT *
FROM cleaned
