-- device-001 … device-100
INSERT INTO devices (device_id)
SELECT 'device-' || lpad(n::text, 3, '0')
FROM generate_series(1, 100) AS n
ON CONFLICT DO NOTHING;
