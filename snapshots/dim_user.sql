
{% snapshot dim_user %}
{{
  config(
    unique_key='sf_user_id',
    strategy='timestamp',
    updated_at='last_modified_date'
  )
}}


 SELECT
    u.user_id AS sf_user_id,
    u.username,
    u.email,
    u.first_name,
    u.last_name,
    u.is_active,
    r.user_role_id,  
    u.created_date,
    u.last_modified_date  
FROM {{ ref('user') }} u
LEFT JOIN {{ ref('user_role') }} r 
    ON u.user_role_id = r.user_role_id
{% endsnapshot %}

