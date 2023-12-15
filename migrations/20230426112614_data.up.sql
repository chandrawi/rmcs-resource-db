CREATE TABLE IF NOT EXISTS "data" (
  "device_id" uuid NOT NULL,
  "model_id" uuid NOT NULL,
  "timestamp" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "data" bytea NOT NULL,
  PRIMARY KEY ("timestamp","model_id","device_id"),
  FOREIGN KEY ("device_id")
    REFERENCES "device" ("device_id"),
  FOREIGN KEY ("model_id")
    REFERENCES "model" ("model_id")
);
