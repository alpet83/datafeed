-- Runtime bootstrap template for binance exchange database.
CREATE DATABASE IF NOT EXISTS `binance` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE `binance`;

CREATE TABLE IF NOT EXISTS `data_config` (
  `id_ticker` int(11) NOT NULL,
  `load_candles` int(11) NOT NULL DEFAULT 0,
  `load_depth` int(11) NOT NULL DEFAULT 0,
  `load_ticks` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id_ticker`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `ticker_map` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `ticker` varchar(16) NOT NULL,
  `symbol` varchar(24) NOT NULL,
  `pair_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ticker` (`ticker`),
  UNIQUE KEY `symbol` (`symbol`),
  KEY `pair_id` (`pair_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

INSERT INTO `ticker_map` (`id`, `ticker`, `symbol`, `pair_id`) VALUES
(1, 'btcusdt', 'BTCUSDT', 1),
(2, 'ethusdt', 'ETHUSDT', 3),
(3, 'xrpusdt', 'XRPUSDT', 22),
(4, 'solusdt', 'SOLUSDT', 54)
ON DUPLICATE KEY UPDATE
  `ticker` = VALUES(`ticker`),
  `symbol` = VALUES(`symbol`),
  `pair_id` = VALUES(`pair_id`);

INSERT INTO `data_config` (`id_ticker`, `load_candles`, `load_depth`, `load_ticks`) VALUES
(1, 1, 0, 3),
(2, 1, 0, 3),
(3, 1, 0, 3),
(4, 1, 0, 3)
ON DUPLICATE KEY UPDATE
  `load_candles` = VALUES(`load_candles`),
  `load_depth` = VALUES(`load_depth`),
  `load_ticks` = VALUES(`load_ticks`);
