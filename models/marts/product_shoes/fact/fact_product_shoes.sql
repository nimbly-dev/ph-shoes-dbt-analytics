{{  
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['id','dwid']
  ) 
}}

WITH raw_data AS (
  SELECT
    id,
    title,
    subtitle,
    url,
    image,
    price_sale,
    price_original,
    gender,
    age_group,
    brand,
    dwid,
    year,
    month,
    day
  FROM {{ ref('stg_product_shoes') }}
  {% if is_incremental() and var('year', none) is not none %}
    WHERE
      year  = {{ var('year') }}
      AND month = {{ var('month') }}
      AND day   = {{ var('day') }}
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
    dwid,
    year,
    month,
    day,
    id,
    title,
    subtitle,
    url,
    image,
    price_sale,
    price_original,
    gender,
    age_group
  FROM deduped
  WHERE row_num = 1
)

SELECT * FROM final
