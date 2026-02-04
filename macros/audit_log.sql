{% macro log_model_audit(
    status,
    row_count=none,
    start_time=none
) %}

{% if execute %}

    {% set end_time = modules.datetime.datetime.utcnow() %}

    {% if start_time is not none %}
        {% set execution_time =
            (end_time - start_time).total_seconds()
        %}
    {% else %}
        {% set execution_time = none %}
    {% endif %}

    INSERT INTO SALESFORCE_DB.TEST.DBT_MODEL_AUDIT
    (
        RUN_ID,
        MODEL_NAME,
        MODEL_SCHEMA,
        MODEL_DATABASE,
        STATUS,
        ROW_COUNT,
        START_TIME,
        END_TIME,
        EXECUTION_TIME_S,
        RUN_BY
    )
    VALUES
    (
        '{{ invocation_id }}',
        '{{ this.name }}',
        '{{ this.schema }}',
        '{{ this.database }}',
        '{{ status }}',
        {{ row_count if row_count is not none else 'NULL' }},
        {{ "'" ~ start_time ~ "'" if start_time is not none else 'NULL' }},
        '{{ end_time }}',
        {{ execution_time if execution_time is not none else 'NULL' }},
        '{{ target.user }}'
    );

{% endif %}

{% endmacro %}



