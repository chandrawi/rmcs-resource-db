CREATE TABLE `buffer` (
  `id` int(10) UNSIGNED NOT NULL,
  `device_id` bigint(20) UNSIGNED NOT NULL,
  `model_id` int(10) UNSIGNED NOT NULL,
  `timestamp` timestamp(6) NOT NULL DEFAULT current_timestamp(6),
  `index` smallint(5) UNSIGNED NOT NULL,
  `data` varbinary(255) NOT NULL,
  `status` enum('DEFAULT','CONVERT','ANALYZE_GATEWAY','ANALYZE_SERVER','TRANSFER_GATEWAY','TRANSFER_SERVER','BACKUP','DELETE','ERROR') NOT NULL DEFAULT 'DEFAULT'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

ALTER TABLE `buffer`
  ADD PRIMARY KEY (`id`),
  ADD KEY `buffer_device_id` (`device_id`),
  ADD KEY `buffer_model_id` (`model_id`);

ALTER TABLE `buffer`
  MODIFY `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;

ALTER TABLE `buffer`
  ADD CONSTRAINT `buffer_device_id` FOREIGN KEY (`device_id`) REFERENCES `device` (`device_id`),
  ADD CONSTRAINT `buffer_model_id` FOREIGN KEY (`model_id`) REFERENCES `model` (`model_id`);
