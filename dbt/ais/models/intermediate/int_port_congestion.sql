-- How busy each port is right now compared with its own recent normal.
-- PortWatch publishes with a lag, so "now" is each port's latest available date:
-- the last 7 days are compared with the 90 days before them.
-- A ratio above 1 means more port calls than usual (a proxy for congestion).

with activity as (
    select port_id, activity_date, portcalls_total
    from {{ ref('stg_ports_activity') }}
),

latest as (
    select port_id, max(activity_date) as latest_date
    from activity
    group by port_id
),

windows as (
    select
        a.port_id,
        l.latest_date,
        avg(iff(a.activity_date > dateadd('day', -7, l.latest_date),
                a.portcalls_total, null))                         as recent_7d_avg_calls,
        avg(iff(a.activity_date <= dateadd('day', -7, l.latest_date),
                a.portcalls_total, null))                         as baseline_90d_avg_calls
    from activity a
    inner join latest l
        on a.port_id = l.port_id
    where a.activity_date > dateadd('day', -97, l.latest_date)
    group by a.port_id, l.latest_date
)

select
    port_id,
    latest_date                                                   as congestion_as_of_date,
    round(recent_7d_avg_calls, 2)                                 as recent_7d_avg_calls,
    round(baseline_90d_avg_calls, 2)                              as baseline_90d_avg_calls,
    round(recent_7d_avg_calls / nullif(baseline_90d_avg_calls, 0), 2) as congestion_ratio,
    case
        when baseline_90d_avg_calls is null or baseline_90d_avg_calls = 0 then 'Unknown'
        when recent_7d_avg_calls / baseline_90d_avg_calls >= 1.2 then 'High'
        when recent_7d_avg_calls / baseline_90d_avg_calls <= 0.8 then 'Low'
        else 'Normal'
    end                                                           as congestion_level
from windows
