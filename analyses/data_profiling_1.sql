
-- 1. Data Profiling: General overview of the raw data
SELECT 
    COUNT(*) AS total_row_count,
    COUNT(DISTINCT device_id) AS unique_device_count,
    MIN(recorded_at) AS data_start_date,
    MAX(recorded_at) AS data_end_date,
    DATEDIFF(MAX(recorded_at), MIN(recorded_at)) AS day_range
FROM {{ ref('stg_device_telemetry') }};
