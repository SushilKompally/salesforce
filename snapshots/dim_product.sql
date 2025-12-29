
{% snapshot dim_product %}
{{
  config(
    unique_key='sf_product_id',
    strategy='timestamp',
    updated_at='last_modified_date',
  )
}}


  SELECT
    product_id AS sf_product_id,
    name,
    product_code,
    family,
    is_active,
    last_modified_date
FROM {{ ref('product') }} 
{% endsnapshot %}

