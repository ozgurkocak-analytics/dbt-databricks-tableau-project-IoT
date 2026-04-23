

-- 2. Profiling using metrics
{% set metrics = ['battery_level', 'temperature_celsius', 'movement_intensity'] %}

{% for metric in metrics %}
SELECT 
    '{{ metric }}' AS metric_name,
    MIN({{ metric }}) AS min_val,
    MAX({{ metric }}) AS max_val,
    AVG({{ metric }}) AS mean_val,
    STDDEV({{ metric }}) AS std_dev
FROM {{ ref('stg_device_telemetry') }}
{% if not loop.last %} UNION ALL {% endif %}
{% endfor %};
