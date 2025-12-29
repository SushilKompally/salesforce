

{% snapshot dim_campaign %}
{{
  config(
    unique_key='sf_campaign_id',
    strategy='timestamp',
    updated_at='last_modified_date',
  )
}}

  SELECT
    campaign_id as sf_campaign_id,
    name,
    entity_type,
    status,
    start_date,
    end_date,
    owner_user_id,
    last_modified_date
 FROM {{ ref('campaign') }}   
{% endsnapshot %}

