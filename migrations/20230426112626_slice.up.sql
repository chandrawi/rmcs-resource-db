CREATE TABLE `slice_index` (
  `id` int(10) UNSIGNED NOT NULL,
  `device_id` bigint(20) UNSIGNED NOT NULL,
  `model_id` int(10) UNSIGNED NOT NULL,
  `index_begin` int(10) UNSIGNED NOT NULL,
  `index_end` int(10) UNSIGNED NOT NULL,
  `name` varchar(32) NOT NULL,
  `description` varchar(255) NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `slice_timestamp` (
  `id` int(10) UNSIGNED NOT NULL,
  `device_id` bigint(20) UNSIGNED NOT NULL,
  `model_id` int(10) UNSIGNED NOT NULL,
  `timestamp_begin` timestamp NOT NULL DEFAULT current_timestamp(),
  `timestamp_end` timestamp NOT NULL DEFAULT current_timestamp(),
  `name` varchar(32) NOT NULL,
  `description` varchar(255) NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `slice_timestamp_index` (
  `id` int(10) UNSIGNED NOT NULL,
  `device_id` bigint(20) UNSIGNED NOT NULL,
  `model_id` int(10) UNSIGNED NOT NULL,
  `timestamp_begin` timestamp NOT NULL DEFAULT current_timestamp(),
  `timestamp_end` timestamp NOT NULL DEFAULT current_timestamp(),
  `index_begin` int(10) UNSIGNED NOT NULL,
  `index_end` int(10) UNSIGNED NOT NULL,
  `name` varchar(32) NOT NULL,
  `description` varchar(255) NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `slice_timestamp_micros` (
  `id` int(10) UNSIGNED NOT NULL,
  `device_id` bigint(20) UNSIGNED NOT NULL,
  `model_id` int(10) UNSIGNED NOT NULL,
  `timestamp_begin` timestamp(6) NOT NULL DEFAULT current_timestamp(6),
  `timestamp_end` timestamp(6) NOT NULL DEFAULT current_timestamp(6),
  `name` varchar(32) NOT NULL,
  `description` varchar(255) NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

ALTER TABLE `slice_index`
  ADD PRIMARY KEY (`id`),
  ADD KEY `slice_index_device_id` (`device_id`),
  ADD KEY `slice_index_model_id` (`model_id`);

ALTER TABLE `slice_timestamp`
  ADD PRIMARY KEY (`id`),
  ADD KEY `slice_timestamp_device_id` (`device_id`),
  ADD KEY `slice_timestamp_model_id` (`model_id`);

ALTER TABLE `slice_timestamp_index`
  ADD PRIMARY KEY (`id`),
  ADD KEY `slice_timestamp_index_device_id` (`device_id`),
  ADD KEY `slice_timestamp_index_model_id` (`model_id`);

ALTER TABLE `slice_timestamp_micros`
  ADD PRIMARY KEY (`id`),
  ADD KEY `slice_timestamp_micros_device_id` (`device_id`),
  ADD KEY `slice_timestamp_micros_model_id` (`model_id`);

ALTER TABLE `slice_index`
  MODIFY `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

ALTER TABLE `slice_timestamp`
  MODIFY `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

ALTER TABLE `slice_timestamp_index`
  MODIFY `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

ALTER TABLE `slice_timestamp_micros`
  MODIFY `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

ALTER TABLE `slice_index`
  ADD CONSTRAINT `slice_index_device_id` FOREIGN KEY (`device_id`) REFERENCES `device` (`device_id`),
  ADD CONSTRAINT `slice_index_model_id` FOREIGN KEY (`model_id`) REFERENCES `model` (`model_id`);

ALTER TABLE `slice_timestamp`
  ADD CONSTRAINT `slice_timestamp_device_id` FOREIGN KEY (`device_id`) REFERENCES `device` (`device_id`),
  ADD CONSTRAINT `slice_timestamp_model_id` FOREIGN KEY (`model_id`) REFERENCES `model` (`model_id`);

ALTER TABLE `slice_timestamp_index`
  ADD CONSTRAINT `slice_timestamp_index_device_id` FOREIGN KEY (`device_id`) REFERENCES `device` (`device_id`),
  ADD CONSTRAINT `slice_timestamp_index_model_id` FOREIGN KEY (`model_id`) REFERENCES `model` (`model_id`);

ALTER TABLE `slice_timestamp_micros`
  ADD CONSTRAINT `slice_timestamp_micros_device_id` FOREIGN KEY (`device_id`) REFERENCES `device` (`device_id`),
  ADD CONSTRAINT `slice_timestamp_micros_model_id` FOREIGN KEY (`model_id`) REFERENCES `model` (`model_id`);
