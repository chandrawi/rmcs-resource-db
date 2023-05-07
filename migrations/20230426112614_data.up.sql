CREATE TABLE `data_timestamp` (
  `device_id` bigint(20) UNSIGNED NOT NULL,
  `model_id` int(10) UNSIGNED NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT current_timestamp(),
  `data` varbinary(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `data_timestamp_index` (
  `device_id` bigint(20) UNSIGNED NOT NULL,
  `model_id` int(10) UNSIGNED NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT current_timestamp(),
  `index` smallint(5) UNSIGNED NOT NULL,
  `data` varbinary(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `data_timestamp_micros` (
  `device_id` bigint(20) UNSIGNED NOT NULL,
  `model_id` int(10) UNSIGNED NOT NULL,
  `timestamp` timestamp(6) NOT NULL DEFAULT current_timestamp(6),
  `data` varbinary(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

ALTER TABLE `data_timestamp`
  ADD PRIMARY KEY (`timestamp`,`model_id`,`device_id`),
  ADD KEY `data_timestamp_device_id` (`device_id`),
  ADD KEY `data_timestamp_model_id` (`model_id`);

ALTER TABLE `data_timestamp_index`
  ADD PRIMARY KEY (`index`,`timestamp`,`model_id`,`device_id`),
  ADD KEY `data_timestamp_index_device_id` (`device_id`),
  ADD KEY `data_timestamp_index_model_id` (`model_id`);

ALTER TABLE `data_timestamp_micros`
  ADD PRIMARY KEY (`timestamp`,`model_id`,`device_id`),
  ADD KEY `data_timestamp_micros_device_id` (`device_id`),
  ADD KEY `data_timestamp_micros_model_id` (`model_id`);

ALTER TABLE `data_timestamp`
  ADD CONSTRAINT `data_timestamp_device_id` FOREIGN KEY (`device_id`) REFERENCES `device` (`device_id`),
  ADD CONSTRAINT `data_timestamp_model_id` FOREIGN KEY (`model_id`) REFERENCES `model` (`model_id`);

ALTER TABLE `data_timestamp_index`
  ADD CONSTRAINT `data_timestamp_index_device_id` FOREIGN KEY (`device_id`) REFERENCES `device` (`device_id`),
  ADD CONSTRAINT `data_timestamp_index_model_id` FOREIGN KEY (`model_id`) REFERENCES `model` (`model_id`);

ALTER TABLE `data_timestamp_micros`
  ADD CONSTRAINT `data_timestamp_micros_device_id` FOREIGN KEY (`device_id`) REFERENCES `device` (`device_id`),
  ADD CONSTRAINT `data_timestamp_micros_model_id` FOREIGN KEY (`model_id`) REFERENCES `model` (`model_id`);
