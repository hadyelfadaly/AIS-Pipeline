{{ config(materialized='table') }}

with stg_ports as (
    select * from {{ ref('stg_ports_ref') }}
)

select
    port_id,
    un_locode,
    port_name,
    full_name,
    country,
    iso3_code,
    continent,
    latitude,
    longitude,
    vessel_count_total,
    industry_top1,
    share_country_maritime_import,
    share_country_maritime_export
from stg_ports
