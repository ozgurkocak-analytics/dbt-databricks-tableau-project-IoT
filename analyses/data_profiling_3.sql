

-- 3. Gold
SELECT * FROM {{ ref('dim_device_risk_profile') }} ORDER BY device_id;