CREATE TABLE IF NOT EXISTS `download_schedule` (
    `date` date NOT NULL,
    `kind` varchar(16) NOT NULL,
    `ticker` varchar(20) NOT NULL,
    `target_volume` float NOT NULL,
    `target_close` float NOT NULL,
    PRIMARY KEY (`date`,`kind`,`ticker`) USING BTREE
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;