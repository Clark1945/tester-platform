-- 可重複執行；既有 readings 的歷史名稱未知，保留 NULL，不以現在名稱偽造歷史。
BEGIN;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS name text NOT NULL DEFAULT '';
UPDATE devices SET name = device_id WHERE name = '';
ALTER TABLE readings ADD COLUMN IF NOT EXISTS device_name text;
COMMIT;
