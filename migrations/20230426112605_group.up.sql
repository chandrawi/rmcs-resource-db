CREATE TABLE IF NOT EXISTS "group_model" (
  "group_id" uuid NOT NULL,
  "name" varchar(32) NOT NULL,
  "category" varchar(64) NOT NULL,
  "description" varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY ("group_id")
);

CREATE TABLE IF NOT EXISTS "group_device" (
  "group_id" uuid NOT NULL,
  "name" varchar(32) NOT NULL,
  "kind" boolean NOT NULL DEFAULT false,
  "category" varchar(64) NOT NULL,
  "description" varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY ("group_id")
);

CREATE TABLE IF NOT EXISTS "group_model_map" (
  "group_id" uuid NOT NULL,
  "model_id" uuid NOT NULL,
  PRIMARY KEY ("group_id","model_id"),
  FOREIGN KEY ("group_id")
    REFERENCES "group_model" ("group_id") ON UPDATE CASCADE ON DELETE CASCADE,
  FOREIGN KEY ("model_id")
    REFERENCES "model" ("model_id") ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS "group_device_map" (
  "group_id" uuid NOT NULL,
  "device_id" uuid NOT NULL,
  PRIMARY KEY ("group_id","device_id"),
  FOREIGN KEY ("group_id")
    REFERENCES "group_device" ("group_id") ON UPDATE CASCADE ON DELETE CASCADE,
  FOREIGN KEY ("device_id")
    REFERENCES "device" ("device_id") ON UPDATE CASCADE ON DELETE CASCADE
);
