
{% snapshot dim_account %}
{{
  config(
    unique_key='sf_account_id',
    strategy='timestamp',
    updated_at='last_modified_date', 
  )
}}

  SELECT
    account_id AS sf_account_id,
    name,
    account_number,
    entity_type,
    industry,
    annual_revenue,
    number_of_employees,
    owner_user_id,
    billing_city,
    shipping_city,
    created_date,
    last_modified_date
FROM {{ ref('account') }}   
{% endsnapshot %}

