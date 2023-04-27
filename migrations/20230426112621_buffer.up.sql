CREATE TABLE `buffer_index` (
  `id` int(10) UNSIGNED NOT NULL,
  `device_id` bigint(20) UNSIGNED NOT NULL,
  `model_id` int(10) UNSIGNED NOT NULL,
  `index` int(10) UNSIGNED NOT NULL,
  `data` varbinary(255) NOT NULL,
  `status` enum('DEFAULT','CONVERT','ANALYZE_GATEWAY','ANALYZE_SERVER','TRANSFER_GATEWAY','TRANSFER_SERVER','BACKUP','DELETE','ERROR') NOT NULL DEFAULT 'DEFAULT'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `buffer_timestamp` (
  `id` int(10) UNSIGNED NOT NULL,
  `device_id` bigint(20) UNSIGNED NOT NULL,
  `model_id` int(10) UNSIGNED NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT current_timestamp(),
  `data` varbinary(255) NOT NULL,
  `status` enum('DEFAULT','CONVERT','ANALYZE_GATEWAY','ANALYZE_SERVER','TRANSFER_GATEWAY','TRANSFER_SERVER','BACKUP','DELETE','ERROR') NOT NULL DEFAULT 'DEFAULT'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `buffer_timestamp_index` (
  `id` int(10) UNSIGNED NOT NULL,
  `device_id` bigint(20) UNSIGNED NOT NULL,
  `model_id` int(10) UNSIGNED NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT current_timestamp(),
  `index` int(10) UNSIGNED NOT NULL,
  `data` varbinary(255) NOT NULL,
  `status` enum('DEFAULT','CONVERT','ANALYZE_GATEWAY','ANALYZE_SERVER','TRANSFER_GATEWAY','TRANSFER_SERVER','BACKUP','DELETE','ERROR') NOT NULL DEFAULT 'DEFAULT'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `buffer_timestamp_micros` (
  `id` int(10) UNSIGNED NOT NULL,
  `device_id` bigint(20) UNSIGNED NOT NULL,
  `model_id` int(10) UNSIGNED NOT NULL,
  `timestamp` timestamp(6) NOT NULL DEFAULT current_timestamp(6),
  `data` varbinary(255) NOT NULL,
  `status` enum('DEFAULT','CONVERT','ANALYZE_GATEWAY','ANALYZE_SERVER','TRANSFER_GATEWAY','TRANSFER_SERVER','BACKUP','DELETE','ERROR') NOT NULL DEFAULT 'DEFAULT'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

ALTER TABLE `buffer_index`
  ADD PRIMARY KEY (`id`),
  ADD KEY `buffer_index_device_id` (`device_id`),
  ADD KEY `buffer_index_model_id` (`model_id`);

ALTER TABLE `buffer_timestamp`
  ADD PRIMARY KEY (`id`),
  ADD KEY `buffer_timestamp_device_id` (`device_id`),
  ADD KEY `buffer_timestamp_model_id` (`model_id`);

ALTER TABLE `buffer_timestamp_index`
  ADD PRIMARY KEY (`id`),
  ADD KEY `buffer_timestamp_index_device_id` (`device_id`),
  ADD KEY `buffer_timestamp_index_model_id` (`model_id`);

ALTER TABLE `buffer_timestamp_micros`
  ADD PRIMARY KEY (`id`),
  ADD KEY `buffer_timestamp_micros_device_id` (`device_id`),
  ADD KEY `buffer_timestamp_micros_model_id` (`model_id`);

ALTER TABLE `buffer_index`
  MODIFY `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

ALTER TABLE `buffer_timestamp`
  MODIFY `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

ALTER TABLE `buffer_timestamp_index`
  MODIFY `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

ALTER TABLE `buffer_timestamp_micros`
  MODIFY `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

ALTER TABLE `buffer_index`
  ADD CONSTRAINT `buffer_index_device_id` FOREIGN KEY (`device_id`) REFERENCES `device` (`device_id`),
  ADD CONSTRAINT `buffer_index_model_id` FOREIGN KEY (`model_id`) REFERENCES `model` (`model_id`);

ALTER TABLE `buffer_timestamp`
  ADD CONSTRAINT `buffer_timestamp_device_id` FOREIGN KEY (`device_id`) REFERENCES `device` (`device_id`),
  ADD CONSTRAINT `buffer_timestamp_model_id` FOREIGN KEY (`model_id`) REFERENCES `model` (`model_id`);

ALTER TABLE `buffer_timestamp_index`
  ADD CONSTRAINT `buffer_timestamp_index_device_id` FOREIGN KEY (`device_id`) REFERENCES `device` (`device_id`),
  ADD CONSTRAINT `buffer_timestamp_index_model_id` FOREIGN KEY (`model_id`) REFERENCES `model` (`model_id`);

ALTER TABLE `buffer_timestamp_micros`
  ADD CONSTRAINT `buffer_timestamp_micros_device_id` FOREIGN KEY (`device_id`) REFERENCES `device` (`device_id`),
  ADD CONSTRAINT `buffer_timestamp_micros_model_id` FOREIGN KEY (`model_id`) REFERENCES `model` (`model_id`);
