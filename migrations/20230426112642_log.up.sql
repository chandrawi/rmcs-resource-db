CREATE TABLE IF NOT EXISTS "system_log" (
  "device_id" uuid NOT NULL,
  "timestamp" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "status" smallint NOT NULL DEFAULT 0,
  "value" bytea NOT NULL,
  "type" smallint NOT NULL DEFAULT 0,
  PRIMARY KEY ("device_id","timestamp")
);
