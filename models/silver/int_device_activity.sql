{{ config(schema='silver') }}

/* This model calculates the difference in battery levels between consecutive pings 
    and determines if a device is within the Richmond Park safe zone.
*/

with telemetry as (
    select * from {{ ref('stg_device_telemetry') }}
),

battery_logic as (
    select
        *,
        -- Fetch the battery level from the previous ping to calculate the delta
        lag(battery_level) over (partition by device_id order by recorded_at) as prev_battery,
        
        -- Geofencing: Determine if coordinates are within Richmond Park boundaries
        -- Center: 51.4450, -0.2725 | Buffer: +/- 0.03
        case 
            when latitude between 51.4150 and 51.4750 
                 and longitude between -0.3025 and -0.2425 
            then 'Inside'
            else 'Outside'
        end as zone_status,

        -- Classify activity based on movement intensity (0-100 scale)
        case 
            when movement_intensity < 20 then 'Stationary'
            when movement_intensity between 20 and 80 then 'Moderate Activity'
            else 'High Intensity'
        end as activity_type,
        
        -- Data frequency context (6 minutes per ping)
        6 as interval_minutes
    from telemetry
),

calculations as (
    select
        *,
        -- Net Change: Current - Previous (Positive = Recharge | Negative = Consumption)
        round(battery_level - coalesce(prev_battery, battery_level), 2) as battery_net_change
    from battery_logic
)

select * from calculations