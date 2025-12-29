
-- macros/incremental_filter.sql
{% macro incremental_filter(
    source_ts_col=None,
    target_ts_col=None,
    lookback_days=None,
    target_relation=None
) -%}
  {# Read defaults from vars if not passed explicitly #}
  {% set source_ts = source_ts_col or var('source_record_creation_column', 'lastmodifieddate') %}
  {% set target_ts = target_ts_col or var('silver_target_timestamp_col', 'last_modified_date') %}
  {% set lb_days   = lookback_days if lookback_days is not none else var('incremental_lookback_days', 1) %}

  {% if is_incremental() %}
    AND {{ source_ts }}::timestamp_ntz >
      (
        SELECT dateadd(
          day,
          -{{ lb_days }},
          coalesce(max({{ target_ts }}), '1900-01-01'::timestamp_ntz)
        )
        FROM {{ target_relation if target_relation is not none else this }}
      )
  {% endif %}
{%- endmacro%}