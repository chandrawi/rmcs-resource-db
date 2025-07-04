CREATE TABLE IF NOT EXISTS "slice_data" (
  "id" serial NOT NULL,
  "device_id" uuid NOT NULL,
  "model_id" uuid NOT NULL,
  "timestamp_begin" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "timestamp_end" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "name" varchar(128) NOT NULL,
  "description" text NOT NULL DEFAULT '',
  PRIMARY KEY ("id"),
  FOREIGN KEY ("device_id")
    REFERENCES "device" ("device_id"),
  FOREIGN KEY ("model_id")
    REFERENCES "model" ("model_id")
);

CREATE TABLE IF NOT EXISTS "slice_data_set" (
  "id" serial NOT NULL,
  "set_id" uuid NOT NULL,
  "timestamp_begin" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "timestamp_end" timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "name" varchar(128) NOT NULL,
  "description" text NOT NULL DEFAULT '',
  PRIMARY KEY ("id"),
  FOREIGN KEY ("set_id")
    REFERENCES "set" ("set_id")
);
