CREATE TABLE "data_timestamp" (
  "device_id" bigint NOT NULL,
  "model_id" int NOT NULL,
  "timestamp" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "data" bytea NOT NULL,
  PRIMARY KEY ("timestamp","model_id","device_id"),
  FOREIGN KEY ("device_id")
    REFERENCES "device" ("device_id"),
  FOREIGN KEY ("model_id")
    REFERENCES "model" ("model_id")
);

CREATE TABLE "data_timestamp_index" (
  "device_id" bigint NOT NULL,
  "model_id" int NOT NULL,
  "timestamp" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "index" smallint NOT NULL,
  "data" bytea NOT NULL,
  PRIMARY KEY ("index","timestamp","model_id","device_id"),
  FOREIGN KEY ("device_id")
    REFERENCES "device" ("device_id"),
  FOREIGN KEY ("model_id")
    REFERENCES "model" ("model_id")
);

CREATE TABLE "data_timestamp_micros" (
  "device_id" bigint NOT NULL,
  "model_id" int NOT NULL,
  "timestamp" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "data" bytea NOT NULL,
  PRIMARY KEY ("timestamp","model_id","device_id"),
  FOREIGN KEY ("device_id")
    REFERENCES "device" ("device_id"),
  FOREIGN KEY ("model_id")
    REFERENCES "model" ("model_id")
);
