CREATE TABLE `group_model` (
  `group_id` int(10) UNSIGNED NOT NULL,
  `category` enum('APPLICATION','COMMAND','ANALYSIS','OTHER') NOT NULL,
  `name` varchar(32) NOT NULL,
  `description` varchar(255) NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `group_device` (
  `group_id` int(10) UNSIGNED NOT NULL,
  `kind` enum('DEVICE','GATEWAY') NOT NULL,
  `category` enum('APPLICATION','LOCATION','NETWORK','ANALYSIS','PROCESS','OTHER') NOT NULL,
  `name` varchar(32) NOT NULL,
  `description` varchar(255) NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `group_model_map` (
  `group_id` int(10) UNSIGNED NOT NULL,
  `model_id` int(10) UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `group_device_map` (
  `group_id` int(10) UNSIGNED NOT NULL,
  `device_id` bigint(20) UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

ALTER TABLE `group_model`
  ADD PRIMARY KEY (`group_id`);

ALTER TABLE `group_device`
  ADD PRIMARY KEY (`group_id`);

ALTER TABLE `group_model_map`
  ADD PRIMARY KEY (`group_id`,`model_id`),
  ADD KEY `group_model_model_id` (`model_id`),
  ADD KEY `group_model_group_id` (`group_id`);

ALTER TABLE `group_device_map`
  ADD PRIMARY KEY (`group_id`,`device_id`),
  ADD KEY `group_device_device_id` (`device_id`),
  ADD KEY `group_device_group_id` (`group_id`);

ALTER TABLE `group_model_map`
  ADD CONSTRAINT `group_model_model_id` FOREIGN KEY (`model_id`) REFERENCES `model` (`model_id`),
  ADD CONSTRAINT `group_model_group_id` FOREIGN KEY (`group_id`) REFERENCES `group_model` (`group_id`);

ALTER TABLE `group_device_map`
  ADD CONSTRAINT `group_device_device_id` FOREIGN KEY (`device_id`) REFERENCES `device` (`device_id`),
  ADD CONSTRAINT `group_device_group_id` FOREIGN KEY (`group_id`) REFERENCES `group_device` (`group_id`);
