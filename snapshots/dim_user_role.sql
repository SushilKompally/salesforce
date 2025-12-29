
{% snapshot dim_user_role %}
{{
  config(
    unique_key='sf_user_role_id',
    strategy='timestamp',
    updated_at='last_modified_date',
  )
}}


SELECT
    user_role_id AS sf_user_role_id,
    name AS role_name,
    user_role_id AS parent_role_id,       
    last_modified_date,
FROM {{ ref('user_role') }} 
{% endsnapshot %}

