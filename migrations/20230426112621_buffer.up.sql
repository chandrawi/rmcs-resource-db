CREATE TABLE IF NOT EXISTS "data_buffer" (
  "id" serial NOT NULL,
  "device_id" uuid NOT NULL,
  "model_id" uuid NOT NULL,
  "timestamp" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "index" int NOT NULL DEFAULT 0,
  "data" bytea NOT NULL,
  "status" smallint NOT NULL DEFAULT 0,
  PRIMARY KEY ("id"),
  FOREIGN KEY ("device_id")
    REFERENCES "device" ("device_id"),
  FOREIGN KEY ("model_id")
    REFERENCES "model" ("model_id")
);
