{#------------------------------------------------------------------------------
  Notes:
    - Designed for Snowflake: uses COALESCE(TRY_TO_BOOLEAN(...), FALSE) to avoid TRY_CAST boolean errors.
    - tool_name is normalized to lowercase and validated; raises a compiler error if invalid.
  Macro: source_metadata
  Params:
    - tool_name (string): one of ['fivetran', 'stitch', 'stitch_legacy', 'datastream', 'datastream_append_mode']
    - record_creation_column (string): column/expression representing record creation timestamp (default: 'lastmodifieddate')
  
------------------------------------------------------------------------------#}
{% macro source_metadata(tool_name=var('tool_name'), record_creation_column="lastmodifieddate") -%}

  {# Normalize and validate tool_name #}
  {% set tool_name_lc = tool_name | lower %}
  {% set allowed_tools = ['fivetran', 'stitch', 'stitch_legacy', 'datastream', 'datastream_append_mode'] %}

  {% if tool_name_lc not in allowed_tools %}
    {{ exceptions.raise_compiler_error(
      "source_metadata: invalid tool_name '" ~ tool_name ~ "'. Allowed: " ~ allowed_tools | join(', ')
    ) }}
  {% endif %}

  {% if tool_name_lc == "fivetran" -%}
    -- Types: tool_name:string='fivetran', record_creation_column:string
    LastModifiedDate AS _source_timestamp,
    COALESCE(TRY_TO_BOOLEAN(ISDELETED), FALSE) AS is_deleted

  {%- elif tool_name_lc == "stitch" -%}
    -- Types: tool_name:string='stitch', record_creation_column:string
    _sdc_received_at AS _source_timestamp,
    IFF(_sdc_deleted_at IS NULL, FALSE, TRUE) AS is_deleted

  {%- elif tool_name_lc == "stitch_legacy" -%}
    -- Types: tool_name:string='stitch_legacy', record_creation_column:string
    _sdc_received_at AS _source_timestamp,
    FALSE AS is_deleted

  {%- elif tool_name_lc == "datastream" -%}
    -- Types: tool_name:string='datastream', record_creation_column:string
    TIMESTAMP_MILLIS(datastream_metadata.source_timestamp) AS _source_timestamp,
    FALSE AS is_deleted

  {%- elif tool_name_lc == "datastream_append_mode" -%}
    -- Types: tool_name:string='datastream_append_mode', record_creation_column:string
    TIMESTAMP_MILLIS(datastream_metadata.source_timestamp) AS _source_timestamp,
    datastream_metadata.change_sequence_number AS change_sequence_number,
    (
      DATEDIFF(
        'day',
        {{ record_creation_column }},
        TIMESTAMP_MILLIS(datastream_metadata.source_timestamp)
      ) <= 90
      AND datastream_metadata.change_type = 'DELETE'
       ) AS is_deleted

  {%- endif %}
{% endmacro %}