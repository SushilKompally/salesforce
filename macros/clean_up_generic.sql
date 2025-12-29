
{# ---------------------------------------------------------
# Strings: trim + NULL if empty
# --------------------------------------------------------- #}
{% macro clean_string(col, tool_name='snowflake') -%}
nullif(trim(to_varchar({{ col }})), '')
{%- endmacro %}

{% macro clean_string_lower(col, tool_name='snowflake') -%}
lower(nullif(trim(to_varchar({{ col }})), ''))
{%- endmacro %}


{# ---------------------------------------------------------
# Numerics: safe integer/decimal
# --------------------------------------------------------- #}
{% macro safe_integer(col, precision=38, scale=3, tool_name='snowflake') -%}
try_to_number({{ col }}, {{ precision }}, {{ scale }})
{%- endmacro %}

{% macro safe_decimal(col, precision=18, scale=2, tool_name='snowflake') -%}
try_to_decimal({{ col }}, {{ precision }}, {{ scale }})
{%- endmacro %}


{# ---------------------------------------------------------
# Date: guard blanks/invalid and cutoff
# --------------------------------------------------------- #}
{% macro safe_date(col, cutoff_date="1900-01-01", tool_name='snowflake') -%}
case
  when nullif(trim({{ col }}), '') is null then null
  when try_to_date({{ col }}) is null then null
  when try_to_date({{ col }}) <= to_date('{{ cutoff_date }}') then null
  else try_to_date({{ col }})
end
{%- endmacro %}


{# ---------------------------------------------------------
# Timestamp (TIMESTAMP_NTZ): guard blanks/invalid
# --------------------------------------------------------- #}
{% macro safe_timestamp_ntz(col, tool_name='snowflake') -%}
case
  when nullif(trim({{ col }}), '') is null then null
  when try_to_timestamp_ntz({{ col }}) is null then null
  else try_to_timestamp_ntz({{ col }})
end
{%- endmacro %}