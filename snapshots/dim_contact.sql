
{% snapshot dim_contact %}
{{
  config(
    unique_key='sf_contact_id',
    strategy='timestamp',
    updated_at='last_modified_date',
  )
}}


  SELECT
   c.contact_id as sf_contact_id,
    da.account_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.created_date,
    c.last_modified_date
FROM {{ ref('contact') }} c
LEFT JOIN {{ ref('account') }} da
    ON c.account_id = da.account_id   
{% endsnapshot %}

