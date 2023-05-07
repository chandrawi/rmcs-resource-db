CREATE TABLE `slice_data` (
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

ALTER TABLE `slice_data`
  ADD PRIMARY KEY (`id`),
  ADD KEY `slice_device_id` (`device_id`),
  ADD KEY `slice_model_id` (`model_id`);

ALTER TABLE `slice_data`
  MODIFY `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

ALTER TABLE `slice_data`
  ADD CONSTRAINT `slice_device_id` FOREIGN KEY (`device_id`) REFERENCES `device` (`device_id`),
  ADD CONSTRAINT `slice_model_id` FOREIGN KEY (`model_id`) REFERENCES `model` (`model_id`);
