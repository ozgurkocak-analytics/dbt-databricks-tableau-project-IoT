-- This test identifies cases where battery level increases slightly
-- which is physically impossible for a tracking device without a full recharge.
-- Legitimate recharges are usually large jumps.

with battery_changes as (
    select
        device_id,
        recorded_at,
        battery_level,
        lag(battery_level) over (partition by device_id order by recorded_at) as prev_battery_level
    from {{ ref('stg_device_telemetry') }}
)

select 
    *
from battery_changes
where 
    -- Case: Battery increased
    battery_level > prev_battery_level 
    -- Exclusion: It's not a significant recharge (e.g., jump to nearly full)
    and (battery_level - prev_battery_level) < 20 
    -- Filtering noise: Ignore tiny fluctuations (sensor jitter) if necessary
    and (battery_level - prev_battery_level) > 0.5