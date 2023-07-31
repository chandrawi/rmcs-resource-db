CREATE TABLE "device_type" (
  "type_id" serial NOT NULL,
  "name" varchar(64) NOT NULL,
  "description" varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY ("type_id")
);

CREATE TABLE "device_type_model" (
  "type_id" int NOT NULL,
  "model_id" int NOT NULL,
  PRIMARY KEY ("type_id","model_id"),
  FOREIGN KEY ("type_id")
    REFERENCES "device_type" ("type_id") ON UPDATE CASCADE ON DELETE CASCADE,
  FOREIGN KEY ("model_id")
    REFERENCES "model" ("model_id") ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE "device" (
  "device_id" bigint NOT NULL,
  "gateway_id" bigint NOT NULL,
  "type_id" int NOT NULL,
  "serial_number" varchar(64) NOT NULL,
  "name" varchar(64) NOT NULL,
  "description" varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY ("device_id"),
  FOREIGN KEY ("type_id")
    REFERENCES "device_type" ("type_id")
);

CREATE TABLE "device_config" (
  "id" serial NOT NULL,
  "device_id" bigint NOT NULL,
  "name" varchar(32) NOT NULL,
  "value" bytea NOT NULL,
  "type" smallint NOT NULL DEFAULT 0,
  "category" varchar(64) NOT NULL,
  PRIMARY KEY ("id"),
  FOREIGN KEY ("device_id")
    REFERENCES "device" ("device_id") ON UPDATE CASCADE ON DELETE CASCADE
);
