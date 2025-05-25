{{ 
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['id','dwid'],
    post_hook=[ 
      export_to_s3(
        this,
        '@PH_SHOES_DB.RAW.S3_FACT_PRODUCT_SHOES_STAGE',
        schema = target.schema ~ '_MARTS'
      )
    ]
  ) 
}}

{{ log("target.schema: " ~ target.schema, info=True) }}

with
    raw_data as (
        select
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
        from {{ ref("stg_product_shoes") }}
        {% if is_incremental() and var("year", none) is not none %}
            where
                year = {{ var("year") }}
                and month = {{ var("month") }}
                and day = {{ var("day") }}
        {% endif %}
    ),

    deduped as (select distinct * from raw_data),

    final as (
        select
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
        from deduped
    )

select *
from final
