CREATE TABLE `model` (
  `model_id` int(10) UNSIGNED NOT NULL,
  `indexing` enum('index','timestamp','timestamp_index','timestamp_micros') NOT NULL,
  `category` enum('UPLINK','DOWNLINK','VIRTUAL','ANALYSIS') NOT NULL,
  `name` varchar(64) NOT NULL,
  `description` varchar(255) NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `model_type` (
  `model_id` int(10) UNSIGNED NOT NULL,
  `index` smallint(5) UNSIGNED NOT NULL,
  `type` enum('i8','i16','i32','i64','u8','u16','u32','u64','f32','f64','char','bool') NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `model_config` (
  `id` int(10) UNSIGNED NOT NULL,
  `model_id` int(10) UNSIGNED NOT NULL,
  `index` smallint(5) UNSIGNED NOT NULL,
  `name` varchar(64) NOT NULL,
  `value` varbinary(255) NOT NULL,
  `type` enum('int','float','str') NOT NULL,
  `category` enum('SCALE','UNIT','SYMBOL','THRESHOLD','OTHER') NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

ALTER TABLE `model`
  ADD PRIMARY KEY (`model_id`);

ALTER TABLE `model_type`
  ADD PRIMARY KEY (`model_id`,`index`);

ALTER TABLE `model_config`
  ADD PRIMARY KEY (`id`),
  ADD KEY `model_config_model_id` (`model_id`);

ALTER TABLE `model`
  MODIFY `model_id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

ALTER TABLE `model_config`
  MODIFY `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

ALTER TABLE `model_type`
  ADD CONSTRAINT `model_type_model_id` FOREIGN KEY (`model_id`) REFERENCES `model` (`model_id`);

ALTER TABLE `model_config`
  ADD CONSTRAINT `model_config_model_id` FOREIGN KEY (`model_id`) REFERENCES `model` (`model_id`);
