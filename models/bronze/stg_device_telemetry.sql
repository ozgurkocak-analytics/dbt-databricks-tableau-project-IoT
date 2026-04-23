with source as (

    select * from {{ source('iot_raw_data', 'device_telemetry') }}
),

cleaned as (
    select
        device_id,
        recorded_at,
        latitude,
        longitude,
        battery_level,
        temperature_celsius,
        movement_intensity,
        connection_status
    from source
   
    where recorded_at <= current_timestamp()
)

select * from cleaned