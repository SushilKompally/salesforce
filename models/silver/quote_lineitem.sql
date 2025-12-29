
/*
-- Description: Incremental Load Script for Silver quote_lineitem - event table
-- Script Name: quote_lineitem_silver.sql
-- Created on: 16-Dec-2025
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load FROM Bronze to Silver for the quote_lineitem table using macros
--     for metadata, cleanup, and incremental filtering (merge strategy).
-- Change History:
--     16-Dec-2025 - Initial creation - Sushil Kompally
*/

{{ config(
    unique_key='quote_line_item_id',
    incremental_strategy='merge',
) }}

WITH raw AS (

    SELECT
        *,
        {{ source_metadata() }}                                  
    FROM {{ source('salesforce_bronze', 'quote_lineitem') }}
    WHERE 1=1
  {{ incremental_filter() }}  

),


cleaned AS (

    SELECT
        -- PRIMARY KEY
        id AS quote_line_item_id,

        -- FOREIGN KEYS
        quoteid    AS quote_id,
        product2id AS product_id,

        -- DETAILS
        quantity    AS quantity,
        unitprice   AS unit_price,
        servicedate AS service_date,
        discount    AS discount,
        totalprice  AS total_price,

        -- AUDIT DATES
        createddate      AS created_date,
        lastmodifieddate AS last_modified_date,

        -- LOAD DATE
        current_timestamp()::timestamp_ntz AS silver_load_date

    FROM raw
)

SELECT *
FROM cleaned
