-- phpMyAdmin SQL Dump
-- version 5.2.1deb3
-- https://www.phpmyadmin.net/
--
-- Хост: localhost:3306
-- Время создания: Апр 21 2025 г., 13:14
-- Версия сервера: 10.11.7-MariaDB-1:10.11.7+maria~ubu2204-log
-- Версия PHP: 8.3.6

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- База данных: `datafeed`
--
CREATE DATABASE IF NOT EXISTS `datafeed` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE `datafeed`;

-- --------------------------------------------------------

--
-- Структура таблицы `cm__listings`
--

CREATE TABLE IF NOT EXISTS `cm__listings` (
  `ts` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `cm_id` int(10) UNSIGNED NOT NULL,
  `symbol` varchar(10) NOT NULL,
  `circulating_supply` float NOT NULL,
  `total_supply` float NOT NULL,
  `supply_coef` float NOT NULL,
  `price` float NOT NULL,
  `volume24_musd` float NOT NULL,
  `volume24_coins` float NOT NULL,
  `gain_daily` float NOT NULL DEFAULT 0 COMMENT 'daily price change in %',
  `gain_weekly` float NOT NULL DEFAULT 0 COMMENT 'weekly price change in %',
  PRIMARY KEY (`ts`,`cm_id`,`symbol`),
  KEY `ts` (`ts`),
  KEY `symbol` (`symbol`),
  KEY `cm_id` (`cm_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin
PARTITION BY RANGE (unix_timestamp(`ts`))
(
PARTITION past VALUES LESS THAN (1609459200) ENGINE=InnoDB,
PARTITION p2021 VALUES LESS THAN (1640995200) ENGINE=InnoDB,
PARTITION p2022 VALUES LESS THAN (1672531200) ENGINE=InnoDB,
PARTITION p2023 VALUES LESS THAN (1704067200) ENGINE=InnoDB,
PARTITION p2024 VALUES LESS THAN (1735689600) ENGINE=InnoDB,
PARTITION p2025 VALUES LESS THAN (1767225600) ENGINE=InnoDB,
PARTITION p2026 VALUES LESS THAN (1798761600) ENGINE=InnoDB,
PARTITION p2027 VALUES LESS THAN (1830297600) ENGINE=InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE=InnoDB
);

-- --------------------------------------------------------

--
-- Структура таблицы `cm__symbols`
--

CREATE TABLE IF NOT EXISTS `cm__symbols` (
  `id` int(11) NOT NULL,
  `symbol` varchar(10) NOT NULL,
  `name` varchar(25) NOT NULL,
  `rank` int(11) DEFAULT NULL,
  `ts_updated` timestamp NOT NULL DEFAULT current_timestamp(),
  `last_price` double DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`,`symbol`),
  KEY `rank` (`rank`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Структура таблицы `cm__symbols_tmp`
--

CREATE TABLE IF NOT EXISTS `cm__symbols_tmp` (
  `id` int(11) NOT NULL,
  `symbol` varchar(10) NOT NULL,
  `name` varchar(25) NOT NULL,
  `rank` int(11) DEFAULT NULL,
  `ts_updated` timestamp NOT NULL DEFAULT current_timestamp(),
  `last_price` double DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`,`symbol`),
  KEY `rank` (`rank`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;