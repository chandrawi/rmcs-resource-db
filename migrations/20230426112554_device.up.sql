CREATE TABLE "device_type" (
  "type_id" uuid NOT NULL,
  "name" varchar(64) NOT NULL,
  "description" varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY ("type_id")
);

CREATE TABLE "device_type_model" (
  "type_id" uuid NOT NULL,
  "model_id" uuid NOT NULL,
  PRIMARY KEY ("type_id","model_id"),
  FOREIGN KEY ("type_id")
    REFERENCES "device_type" ("type_id") ON UPDATE CASCADE ON DELETE CASCADE,
  FOREIGN KEY ("model_id")
    REFERENCES "model" ("model_id") ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE "device" (
  "device_id" uuid NOT NULL,
  "gateway_id" uuid NOT NULL,
  "type_id" uuid NOT NULL,
  "serial_number" varchar(64) NOT NULL,
  "name" varchar(64) NOT NULL,
  "description" varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY ("device_id"),
  FOREIGN KEY ("type_id")
    REFERENCES "device_type" ("type_id")
);

CREATE TABLE "device_config" (
  "id" serial NOT NULL,
  "device_id" uuid NOT NULL,
  "name" varchar(32) NOT NULL,
  "value" bytea NOT NULL,
  "type" smallint NOT NULL DEFAULT 0,
  "category" varchar(64) NOT NULL,
  PRIMARY KEY ("id"),
  FOREIGN KEY ("device_id")
    REFERENCES "device" ("device_id") ON UPDATE CASCADE ON DELETE CASCADE
);
