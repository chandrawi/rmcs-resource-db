CREATE TABLE IF NOT EXISTS "data_buffer" (
  "id" serial NOT NULL,
  "device_id" uuid NOT NULL,
  "model_id" uuid NOT NULL,
  "timestamp" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "data" bytea NOT NULL,
  "status" smallint NOT NULL DEFAULT 0,
  PRIMARY KEY ("timestamp","model_id","device_id"),
  FOREIGN KEY ("device_id")
    REFERENCES "device" ("device_id"),
  FOREIGN KEY ("model_id")
    REFERENCES "model" ("model_id")
);
