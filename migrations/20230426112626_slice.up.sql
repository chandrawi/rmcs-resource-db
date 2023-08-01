CREATE TABLE "data_slice" (
  "id" serial NOT NULL,
  "device_id" bigint NOT NULL,
  "model_id" int NOT NULL,
  "timestamp_begin" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "timestamp_end" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "index_begin" int NOT NULL DEFAULT 0,
  "index_end" int NOT NULL DEFAULT 0,
  "name" varchar(32) NOT NULL,
  "description" varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY ("id"),
  FOREIGN KEY ("device_id")
    REFERENCES "device" ("device_id"),
  FOREIGN KEY ("model_id")
    REFERENCES "model" ("model_id")
);
