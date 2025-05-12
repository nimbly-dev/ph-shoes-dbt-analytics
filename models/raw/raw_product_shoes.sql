{{ config(materialized='view') }}

select
  $1:id::string      as id,
  $1:title::string   as title,
  $1:subTitle::string as subTitle,
  $1:url::string     as url,
  $1:image::string   as image,
  $1:price_sale::float as price_sale,
  $1:price_original::float as price_original,
  $1:gender::array     as gender,
  $1:age_group::string as age_group,
  $1:brand::string     as brand,
  object_delete(
    $1,
    'id','title','subTitle','url','image','price_sale','price_original','gender','age_group','brand'
  )::variant as extra
from @raw_product_shoes_stage (file_format => 'csv_raw_format') t;
