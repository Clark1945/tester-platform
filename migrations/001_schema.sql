CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS devices (
  device_id   text PRIMARY KEY,
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS readings (
  uni_id       uuid        NOT NULL,   -- 即 request_id，重送時必須相同
  device_id    text        NOT NULL REFERENCES devices(device_id),
  seq          bigint      NOT NULL,   -- per-device 遞增序號，從 1 開始，重送沿用
  record_time  timestamptz NOT NULL,   -- device 產生，UTC，重送時不變
  freq         integer     NOT NULL,
  ingested_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (uni_id, record_time)         -- hypertable 的 unique 必須包含分區欄位
);

SELECT create_hypertable('readings', 'record_time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS readings_device_seq_idx ON readings (device_id, seq);
