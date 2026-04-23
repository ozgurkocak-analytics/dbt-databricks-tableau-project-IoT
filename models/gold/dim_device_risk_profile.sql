{{ config(schema='gold') }}

/* This dimension model categorizes devices into risk segments based on 
   battery health, geofence compliance, and signal reliability.
*/

with risk_metrics as (
    select
        device_id,
        min(battery_level) as lowest_battery,
        -- Calculate the ratio of weak signals (Optimized threshold for the 'Critical' group)
        avg(case when connection_status = 'weak' then 1.0 else 0.0 end) as weak_signal_ratio,
        -- Count unique days where the device left the safe zone
        count(distinct case when zone_status = 'Outside' then date(recorded_at) end) as violation_days
    from {{ ref('int_device_activity') }}
    group by 1
),

flags as (
    select 
        *,
        -- Thresholds defined based on simulation data patterns
        (lowest_battery < 15) as is_low_battery,         -- Flags BadBattery & Critical groups
        (violation_days > 10) as is_frequent_wanderer,  -- Flags Wanderer & Critical groups
        (weak_signal_ratio > 0.10) as is_weak_connection -- Specifically flags the Critical group
    from risk_metrics
)

select 
    *,
    -- Concatenate risk factors for a readable summary (Spark SQL compatible)
    trim(
        concat_ws(' & ', 
            case when is_low_battery then 'Low Battery' else null end,
            case when is_frequent_wanderer then 'Security Risk' else null end,
            case when is_weak_connection then 'Signal Issue' else null end
        )
    ) as risk_factors,
    
    -- Final Operational Status Classification
    case 
        when is_low_battery and is_frequent_wanderer then 'CRITICAL: Multi-Risk'
        when is_low_battery then 'MAINTENANCE: Low Battery'
        when is_frequent_wanderer then 'SECURITY: Outside Zone'
        when not (is_low_battery or is_frequent_wanderer or is_weak_connection) then 'HEALTHY'
        else 'ACTION REQUIRED'
    end as operational_status
from flags