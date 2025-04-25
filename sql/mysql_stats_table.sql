CREATE TABLE IF NOT EXISTS @TABLENAME (
  `date` date NOT NULL,
  `day_start` timestamp(3) NOT NULL COMMENT 'Timestamp of first tick/minute',
  `day_end` timestamp(3) NOT NULL COMMENT 'Timestamp of last tick/minute',
  `volume_minutes` float DEFAULT NULL,
  `volume_day` float DEFAULT NULL,
  `count_minutes` int(11) NOT NULL DEFAULT 0,
  `count_ticks` int(11) NOT NULL DEFAULT 0,
  `repairs` int(11) NOT NULL DEFAULT 0,
  `flags` int(11) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;