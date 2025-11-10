CREATE TABLE IF NOT EXISTS "set_template" (
  "template_id" uuid NOT NULL,
  "name" varchar(128) NOT NULL,
  "description" text NOT NULL DEFAULT '',
  PRIMARY KEY ("template_id")
);

CREATE TABLE IF NOT EXISTS "set_template_map" (
  "template_id" uuid NOT NULL,
  "type_id" uuid NOT NULL,
  "model_id" uuid NOT NULL,
  "data_index" bytea,
  "template_index" smallint NOT NULL DEFAULT 0,
  PRIMARY KEY ("template_id","template_index"),
  FOREIGN KEY ("template_id")
    REFERENCES "set_template" ("template_id") ON UPDATE CASCADE ON DELETE CASCADE,
  FOREIGN KEY ("type_id")
    REFERENCES "device_type" ("type_id") ON UPDATE CASCADE ON DELETE CASCADE,
  FOREIGN KEY ("model_id")
    REFERENCES "model" ("model_id") ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS "set" (
  "set_id" uuid NOT NULL,
  "template_id" uuid NOT NULL,
  "name" varchar(128) NOT NULL,
  "description" text NOT NULL DEFAULT '',
  PRIMARY KEY ("set_id"),
  FOREIGN KEY ("template_id")
    REFERENCES "set_template" ("template_id") ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS "set_map" (
  "set_id" uuid NOT NULL,
  "device_id" uuid NOT NULL,
  "model_id" uuid NOT NULL,
  "data_index" bytea,
  "set_position" smallint NOT NULL DEFAULT 0,
  "set_number" smallint NOT NULL DEFAULT 0,
  PRIMARY KEY ("set_id","device_id","model_id"),
  FOREIGN KEY ("set_id")
    REFERENCES "set" ("set_id") ON UPDATE CASCADE ON DELETE CASCADE,
  FOREIGN KEY ("device_id")
    REFERENCES "device" ("device_id") ON UPDATE CASCADE ON DELETE CASCADE,
  FOREIGN KEY ("model_id")
    REFERENCES "model" ("model_id") ON UPDATE CASCADE ON DELETE CASCADE
);
