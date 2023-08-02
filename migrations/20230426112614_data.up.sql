CREATE TABLE "data" (
  "device_id" uuid NOT NULL,
  "model_id" uuid NOT NULL,
  "timestamp" timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "index" int NOT NULL DEFAULT 0,
  "data" bytea NOT NULL,
  PRIMARY KEY ("index","timestamp","model_id","device_id"),
  FOREIGN KEY ("device_id")
    REFERENCES "device" ("device_id"),
  FOREIGN KEY ("model_id")
    REFERENCES "model" ("model_id")
);
