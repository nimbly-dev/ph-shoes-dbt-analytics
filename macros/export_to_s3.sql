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

  {# 1) Default filename #}
  {% if filename is none %}
    {% set filename = relation.identifier %}
  {% endif %}

  {# 2) Resolve and pad year/month/day #}
  {% set y_int = (year   if year   is not none else var('year'))  | int %}
  {% set m_int = (month  if month  is not none else var('month')) | int %}
  {% set d_int = (day    if day    is not none else var('day'))   | int %}

  {% set y = '%04d'|format(y_int) %}
  {% set m = '%02d'|format(m_int) %}
  {% set d = '%02d'|format(d_int) %}

  {# 3) Build S3 key: [prefix/][YYYY]/[MM]/[DD]/filename.parquet #}
  {% set parts = [] %}
  {% if prefix %}
    {% do parts.append(prefix.rstrip('/')) %}
  {% endif %}
  {% do parts.append(y) %}
  {% do parts.append(m) %}
  {% do parts.append(d) %}
  {% do parts.append(filename ~ '.parquet') %}
  {% set key = parts | join('/') %}

  {# 4) Single-file export from the proper schema #}
  COPY INTO {{ stage.rstrip('/') ~ '/' ~ key }}
  FROM {{ relation.database }}.{{ schema }}.{{ relation.identifier }}
  FILE_FORMAT = (TYPE = 'PARQUET')
  SINGLE = TRUE
  OVERWRITE = TRUE
  ;
{% endmacro %}
