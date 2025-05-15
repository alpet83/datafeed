CREATE TABLE IF NOT EXISTS `download_history` (
  `ts` timestamp NOT NULL,
  `date` date NOT NULL,
  `kind` varchar(16) NOT NULL,
  `ticker` varchar(20) NOT NULL,
  `count` int(10) UNSIGNED NOT NULL,
  `volume` float NOT NULL,
  `result` VARCHAR(32) NOT NULL,
  KEY `ts` (`ts`),
  KEY `date` (`date`,`kind`,`ticker`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

