CREATE TABLE IF NOT EXISTS "model" (
  "model_id" uuid NOT NULL,
  "name" varchar(128) NOT NULL,
  "category" varchar(64) NOT NULL,
  "description" text NOT NULL DEFAULT '',
  "data_type" bytea,
  PRIMARY KEY ("model_id")
);

CREATE TABLE IF NOT EXISTS "model_config" (
  "id" serial NOT NULL,
  "model_id" uuid NOT NULL,
  "index" smallint NOT NULL,
  "name" varchar(128) NOT NULL,
  "value" bytea NOT NULL,
  "type" smallint NOT NULL DEFAULT 0,
  "category" varchar(64) NOT NULL,
  PRIMARY KEY ("id"),
  FOREIGN KEY ("model_id")
    REFERENCES "model" ("model_id") ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS "model_tag" (
  "model_id" uuid NOT NULL,
  "tag" smallint NOT NULL,
  "name" varchar(128) NOT NULL,
  "members" bytea NOT NULL,
  PRIMARY KEY ("model_id", "tag"),
  FOREIGN KEY ("model_id")
    REFERENCES "model" ("model_id") ON UPDATE CASCADE ON DELETE CASCADE
);
