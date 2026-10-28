{{ config(materialized='table') }}

with activity as (
    select * from {{ ref('stg_ports_activity') }}
),

ports as (
    select port_id from {{ ref('dim_ports') }}
)

select
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['a.port_id', 'a.activity_date']) }} as activity_id,

    -- Foreign Keys
    a.port_id,
    a.activity_date,
    a.activity_year,
    a.activity_month,
    a.activity_quarter,

    -- Metrics
    a.portcalls_total,
    a.portcalls_container,
    a.portcalls_dry_bulk,
    a.portcalls_general_cargo,
    a.portcalls_roro,
    a.portcalls_tanker,

    a.import_total,
    a.export_total,
    (a.import_total + a.export_total) as throughput_total

from activity a
inner join ports p on a.port_id = p.port_id
