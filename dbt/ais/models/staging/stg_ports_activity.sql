{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'ports_activity') }}
),

renamed_and_derived as (
    select
        -- Identifiers & Dates
        portid as port_id,
        date as activity_date,
        year(date) as activity_year,
        month(date) as activity_month,
        quarter(date) as activity_quarter,

        -- Port Calls (Vessel Arrivals)
        coalesce(portcalls_container, 0) as portcalls_container,
        coalesce(portcalls_dry_bulk, 0) as portcalls_dry_bulk,
        coalesce(portcalls_general_cargo, 0) as portcalls_general_cargo,
        coalesce(portcalls_roro, 0) as portcalls_roro,
        coalesce(portcalls_tanker, 0) as portcalls_tanker,
        coalesce(portcalls_cargo, 0) as portcalls_cargo,
        coalesce(portcalls_total, 0) as portcalls_total,

        -- Import Volumes
        coalesce(import_container, 0) as import_container,
        coalesce(import_dry_bulk, 0) as import_dry_bulk,
        coalesce(import_general_cargo, 0) as import_general_cargo,
        coalesce(import_roro, 0) as import_roro,
        coalesce(import_tanker, 0) as import_tanker,
        coalesce(import_cargo, 0) as import_cargo,
        coalesce(import_total, 0) as import_total,

        -- Export Volumes
        coalesce(export_container, 0) as export_container,
        coalesce(export_dry_bulk, 0) as export_dry_bulk,
        coalesce(export_general_cargo, 0) as export_general_cargo,
        coalesce(export_roro, 0) as export_roro,
        coalesce(export_tanker, 0) as export_tanker,
        coalesce(export_cargo, 0) as export_cargo,
        coalesce(export_total, 0) as export_total

    from source
    where date is not null
      and portid is not null
)

select * from renamed_and_derived
