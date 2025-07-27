{{  
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['id','dwid']
  ) 
}}

{% set year_var  = var('year',  none) %}
{% set month_var = var('month', none) %}
{% set day_var   = var('day',   none) %}

WITH
  raw_data AS (
    SELECT
      id, title, subtitle, url, image,
      price_sale, price_original,
      gender, age_group, brand,
      dwid, year, month, day,
      extra
    FROM {{ ref('stg_product_shoes') }}
    {% if is_incremental() and year_var is not none 
                     and month_var is not none 
                     and day_var is not none %}
    WHERE
      year  = {{ year_var }}
      AND month = {{ month_var }}
      AND day   = {{ day_var }}
    {% endif %}
  ),

  deduped AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id, dwid ORDER BY id) AS row_num
    FROM raw_data
  ),

  final AS (
    SELECT
      brand,
      dwid, year, month, day,
      id, title, subtitle, url, image,
      price_sale, price_original,
      gender, age_group,
      extra
    FROM deduped
    WHERE row_num = 1
  )

SELECT * 
FROM final
