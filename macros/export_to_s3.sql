{% macro export_to_s3(
    relation,
    stage,
    schema,
    year=None,
    month=None,
    day=None,
    prefix='',
    filename=None
) %}

  {# 1) Default filename if none provided #}
  {% if filename is none %}
    {% set filename = relation.identifier %}
  {% endif %}

  {# 2) Try args → CLI var → run_started_at #}
  {% set y_arg = year if year is not none else none %}
  {% set m_arg = month if month is not none else none %}
  {% set d_arg = day if day is not none else none %}

  {% set y_var = var('year', none) %}
  {% set m_var = var('month', none) %}
  {% set d_var = var('day', none) %}

  {% set y_fallback = run_started_at.strftime('%Y') %}
  {% set m_fallback = run_started_at.strftime('%m') %}
  {% set d_fallback = run_started_at.strftime('%d') %}

  {% set y_str = (y_arg or y_var or y_fallback) %}
  {% set m_str = (m_arg or m_var or m_fallback) %}
  {% set d_str = (d_arg or d_var or d_fallback) %}

  {% set y = "%04d"|format(y_str|int) %}
  {% set m = "%02d"|format(m_str|int) %}
  {% set d = "%02d"|format(d_str|int) %}

  {# 3) Build the key path #}
  {% set parts = [] %}
  {% if prefix %}
    {% do parts.append(prefix.rstrip('/')) %}
  {% endif %}
  {% do parts.append(y) %}
  {% do parts.append(m) %}
  {% do parts.append(d) %}
  {% do parts.append(filename ~ '.parquet') %}
  {% set key = parts | join('/') %}

  {# 4) Emit COPY command #}
  COPY INTO {{ stage.rstrip('/') ~ '/' ~ key }}
  FROM {{ relation.database }}.{{ schema }}.{{ relation.identifier }}
  FILE_FORMAT = (TYPE = 'PARQUET')
  SINGLE      = TRUE
  OVERWRITE   = TRUE
  ;
{% endmacro %}
