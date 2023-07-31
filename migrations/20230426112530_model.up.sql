CREATE TABLE "model" (
  "model_id" serial NOT NULL,
  "name" varchar(64) NOT NULL,
  "indexing" smallint NOT NULL DEFAULT 0,
  "category" varchar(64) NOT NULL,
  "description" varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY ("model_id")
);

CREATE TABLE "model_type" (
  "model_id" int NOT NULL,
  "index" smallint NOT NULL,
  "type" smallint NOT NULL DEFAULT 0,
  PRIMARY KEY ("model_id","index"),
  FOREIGN KEY ("model_id")
    REFERENCES "model" ("model_id") ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE "model_config" (
  "id" serial NOT NULL,
  "model_id" int NOT NULL,
  "index" smallint NOT NULL,
  "name" varchar(64) NOT NULL,
  "value" bytea NOT NULL,
  "type" smallint NOT NULL DEFAULT 0,
  "category" varchar(64) NOT NULL,
  PRIMARY KEY ("id"),
  FOREIGN KEY ("model_id")
    REFERENCES "model" ("model_id") ON UPDATE CASCADE ON DELETE CASCADE
);
