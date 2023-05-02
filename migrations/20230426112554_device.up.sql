CREATE TABLE `device_type` (
  `type_id` int(10) UNSIGNED NOT NULL,
  `name` varchar(64) NOT NULL,
  `description` varchar(255) NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `device_type_model` (
  `type_id` int(10) UNSIGNED NOT NULL,
  `model_id` int(10) UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `device` (
  `device_id` bigint(20) UNSIGNED NOT NULL,
  `gateway_id` bigint(20) UNSIGNED NOT NULL,
  `type_id` int(10) UNSIGNED NOT NULL,
  `serial_number` varchar(64) NOT NULL,
  `name` varchar(64) NOT NULL,
  `description` varchar(255) NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `device_config` (
  `id` int(10) UNSIGNED NOT NULL,
  `device_id` bigint(20) UNSIGNED NOT NULL,
  `name` varchar(32) NOT NULL,
  `value` varbinary(255) NOT NULL,
  `type` enum('int','float','str') NOT NULL,
  `category` enum('NETWORK','CONVERSION','ANALYSIS','THRESHOLD','OTHER') NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

ALTER TABLE `device_type`
  ADD PRIMARY KEY (`type_id`);

ALTER TABLE `device_type_model`
  ADD PRIMARY KEY (`type_id`,`model_id`);

ALTER TABLE `device`
  ADD PRIMARY KEY (`device_id`),
  ADD UNIQUE KEY `device_serial_number` (`serial_number`),
  ADD KEY `device_type_id` (`type_id`);

ALTER TABLE `device_config`
  ADD PRIMARY KEY (`id`),
  ADD KEY `device_config_device_id` (`device_id`);

ALTER TABLE `device_type`
  MODIFY `type_id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

ALTER TABLE `device_config`
  MODIFY `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

ALTER TABLE `device_type_model`
  ADD CONSTRAINT `device_type_model_type_id` FOREIGN KEY (`type_id`) REFERENCES `device_type` (`type_id`),
  ADD CONSTRAINT `device_type_model_model_id` FOREIGN KEY (`model_id`) REFERENCES `model` (`model_id`);

ALTER TABLE `device`
  ADD CONSTRAINT `device_type_id` FOREIGN KEY (`type_id`) REFERENCES `device_type` (`type_id`);

ALTER TABLE `device_config`
  ADD CONSTRAINT `device_config_device_id` FOREIGN KEY (`device_id`) REFERENCES `device` (`device_id`);
