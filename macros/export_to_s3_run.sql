{% macro export_to_s3_run(export=false, year=None, month=None, day=None) %}

  {# 0) If export flag is false, do nothing #}
  {% if not export %}
    {% do log("export_to_s3_run skipped (export=false)", info=True) %}
    {% do return('') %}
  {% endif %}

  {# 1) Determine each date part: var → arg → run_started_at #}
  {% set y = year if year is not none else run_started_at.strftime('%Y') %}
  {% set m = month if month is not none else run_started_at.strftime('%m') %}
  {% set d = day if day is not none else run_started_at.strftime('%d') %}

  {# 2) Zero-pad month/day and format year #}
  {% set Y = "%04d"|format(y|int) %}
  {% set M = "%02d"|format(m|int) %}
  {% set D = "%02d"|format(d|int) %}

  {# 3) Invoke the stored proc in one statement #}
  CALL PH_SHOES_DB.RAW.SP_EXPORT_FACT_PRODUCT_SHOES(
    '{{ Y }}',
    '{{ M }}',
    '{{ D }}'
  );

{% endmacro %}
