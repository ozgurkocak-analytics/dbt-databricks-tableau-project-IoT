{{ config(schema='gold') }}

/* Daily aggregation of device metrics. 
    Separates actual battery consumption from recharge events for better analytics.
*/

with silver_data as (
    select * from {{ ref('int_device_activity') }}
),

daily_agg as (
    select
        device_id,
        date(recorded_at) as activity_date,
        
        -- Activity Durations (Based on 6-minute ping frequency)
        count(case when activity_type != 'Stationary' then 1 end) * max(interval_minutes) as total_active_minutes,
        count(case when zone_status = 'Outside' then 1 end) * max(interval_minutes) as minutes_outside_safe_zone,
        
        -- Battery Analytics: Calculating real-world usage vs recharging
        -- Sum of all negative changes (Absolute value of total daily energy used)
        abs(sum(case when battery_net_change < 0 then battery_net_change else 0 end)) as daily_consumption_pct,
        
        -- Sum of all significant positive changes (Total energy gained via charging)
        sum(case when battery_net_change > 20 then battery_net_change else 0 end) as daily_recharge_gain_pct,
        
        -- Total number of charging events detected per day
        count(case when battery_net_change > 20 then 1 end) as recharge_event_count,

        -- Sensor & Environmental Data
        avg(temperature_celsius) as avg_daily_temp,
        max(movement_intensity) as peak_movement_intensity
    from silver_data
    group by 1, 2
)

select 
    *,
    -- Operational Efficiency: Percentage of the day the device was active (1440 mins in a day)
    round((total_active_minutes / 1440.0) * 100, 2) as daily_activity_ratio_pct,
    
    -- Security Compliance: Ratio of outside time vs total active time
    case 
        when total_active_minutes > 0 
        then round((minutes_outside_safe_zone / total_active_minutes) * 100, 2) 
        else 0 
    end as violation_intensity_pct
from daily_agg