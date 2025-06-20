{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["id", "dwid"],
        post_hook=[
                "
                -- TEMPORARY FIX: force-set brand from URL; handling of brand SHOULD be on raw extract.
                UPDATE {{ this }}
                SET brand = CASE
                    WHEN LOWER(url) LIKE '%://www.nike.%'               THEN 'nike'
                    WHEN LOWER(url) LIKE '%://www.adidas.%'             THEN 'adidas'
                    WHEN LOWER(url) LIKE '%://atmos.ph/collections/new-balance/%' THEN 'newbalance'
                    WHEN LOWER(url) LIKE '%://worldbalance.%'           THEN 'worldbalance'
                    WHEN LOWER(url) LIKE '%://www.asics.%'              THEN 'asics'
                    WHEN LOWER(url) LIKE '%://www.hoka.%'               THEN 'hoka'
                    ELSE brand
                END
                WHERE LOWER(url) LIKE ANY (
                    '%://www.nike.%',
                    '%://www.adidas.%',
                    '%://atmos.ph/collections/new-balance/%',
                    '%://worldbalance.%',
                    '%://www.asics.%',
                    '%://www.hoka.%'
                );
                "
        ]
    )
}}

with
    raw_csv as (
        select
            id,
            title,
            subtitle,
            url,
            image,
            price_sale,
            price_original,
            gender   as gender_raw,
            age_group,
            brand    as brand_raw,
            loaded_at
        from ph_shoes_db.raw.raw_product_shoes_raw
    ),

    enriched as (
        select
            -- batch identifiers
            to_varchar(date_trunc('day', loaded_at), 'YYYYMMDD') as dwid,
            year(loaded_at)::int  as year,
            month(loaded_at)::int as month,
            day(loaded_at)::int   as day,

            -- raw fields
            id,
            title,
            subtitle,
            url,
            image,
            price_sale,
            price_original,
            age_group,

            case
                when typeof(gender_raw) = 'ARRAY' then gender_raw
                when typeof(gender_raw) = 'STRING'
                     and try_parse_json(gender_raw::string) is not null
                     and typeof(try_parse_json(gender_raw::string)) = 'ARRAY'
                     then try_parse_json(gender_raw::string)
                when typeof(gender_raw) = 'STRING' and gender_raw is not null
                     then array_construct(gender_raw::string)
                else null
            end as gender_arr,

            /* collapse ARRAY → single lowercase string */
            case
                when gender_arr is null            then null
                when array_size(gender_arr) > 1     then 'unisex'
                when array_size(gender_arr) = 1     then lower(gender_arr[0]::string)
                else null
            end as gender,

            brand_raw as brand
        from raw_csv
    ),

    to_load as (
        select
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
            age_group,
            brand
        from enriched

        {% if is_incremental() %}
            where dwid > coalesce((select max(dwid) from {{ this }}), '00000000')
              and price_original <> 0
        {% else %}
            where price_original <> 0
        {% endif %}
    )

select *
from to_load
