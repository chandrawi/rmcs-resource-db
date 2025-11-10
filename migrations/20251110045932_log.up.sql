CREATE TABLE IF NOT EXISTS "system_log" (
  "id" serial NOT NULL,
  "timestamp" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "device_id" uuid,
  "model_id" uuid,
  "tag" smallint NOT NULL DEFAULT 0,
  "value" bytea NOT NULL,
  "type" smallint NOT NULL DEFAULT 0,
  PRIMARY KEY ("id","timestamp")
);
