-- phpMyAdmin SQL Dump
-- version 5.2.1deb3
-- https://www.phpmyadmin.net/
--
-- Хост: localhost:3306
-- Время создания: Апр 27 2025 г., 07:47
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
-- База данных: `bitmex`
--
CREATE DATABASE IF NOT EXISTS `bitmex` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE `bitmex`;

-- --------------------------------------------------------

--
-- Структура таблицы `data_config`
--

CREATE TABLE `data_config` (
  `id_ticker` int(11) NOT NULL,
  `load_candles` int(11) NOT NULL DEFAULT 0,
  `load_depth` int(11) NOT NULL DEFAULT 0,
  `load_ticks` int(11) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin;

--
-- Дамп данных таблицы `data_config`
--

INSERT INTO `data_config` (`id_ticker`, `load_candles`, `load_depth`, `load_ticks`) VALUES
(1, 1, 0, 0),
(2, 1, 0, 0),
(3, 1, 0, 0),
(4, 1, 0, 0),
(5, 1, 0, 0),
(6, 1, 0, 0),
(7, 1, 0, 0),
(8, 1, 0, 0),
(9, 1, 0, 0),
(11, 0, 0, 0),
(12, 1, 0, 3),
(13, 1, 0, 0),
(14, 1, 0, 0),
(15, 1, 0, 0),
(16, 1, 0, 3),
(17, 1, 0, 0),
(18, 1, 0, 3),
(19, 1, 0, 0),
(20, 1, 0, 0),
(21, 1, 0, 0),
(22, 1, 0, 0),
(23, 1, 0, 0),
(24, 1, 0, 0),
(25, 1, 0, 0),
(26, 1, 0, 0),
(27, 0, 0, 0),
(28, 1, 0, 0),
(29, 1, 0, 0),
(30, 1, 0, 0),
(31, 1, 0, 0),
(32, 1, 0, 0),
(33, 3, 0, 0);

-- --------------------------------------------------------

--
-- Структура таблицы `ticker_map`
--

CREATE TABLE `ticker_map` (
  `id` int(11) NOT NULL,
  `ticker` varchar(16) NOT NULL,
  `symbol` varchar(16) NOT NULL,
  `pair_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin;

--
-- Дамп данных таблицы `ticker_map`
--

INSERT INTO `ticker_map` (`id`, `ticker`, `symbol`, `pair_id`) VALUES
(1, 'aaveusd', 'AAVEUSD', 5),
(2, 'adausd', 'ADAUSD', 40),
(3, 'aptusd', 'APTUSD', 8),
(4, 'arbusd', 'ARBUSD', 18),
(5, 'avaxusd', 'AVAXUSD', 17),
(6, 'axsusd', 'AXSUSD', 48),
(7, 'bchusd', 'BCHUSD', 9),
(8, 'dogeusd', 'DOGEUSD', 50),
(9, 'dotusd', 'DOTUSD', 51),
(10, 'eosusd', 'EOSUSD', 14),
(11, 'ethh25', 'ETHH25', 112),
(12, 'ethusd', 'ETHUSD', 3),
(13, 'filusd', 'FILUSD', 16),
(14, 'gmtusd', 'GMTUSD', 11),
(15, 'ldousd', 'LDOUSD', 42),
(16, 'linkusd', 'LINKUSD', 19),
(17, 'ltcusd', 'LTCUSD', 4),
(18, 'nearusd', 'NEARUSD', 25),
(19, 'opusd', 'OPUSD', 21),
(20, 'ordiusd', 'ORDIUSD', 47),
(21, 'pepeusd', 'PEPEUSD', 76),
(22, 'shibusd', 'SHIBUSD', 55),
(23, 'solusd', 'SOLUSD', 54),
(24, 'suiusd', 'SUIUSD', 24),
(25, 'tonusd', 'TONUSD', 29),
(26, 'wldusd', 'WLDUSD', 49),
(27, 'btch25', 'XBTH25', 101),
(28, 'btcm25', 'XBTM25', 102),
(29, 'btcu25', 'XBTU25', 103),
(30, 'btcusd', 'XBTUSD', 1),
(31, 'xrpusd', 'XRPUSD', 22),
(32, 'btceur', 'XBTEUR', 91),
(33, 'dydxusd', 'DYDXUSD', 66);

--
-- Индексы сохранённых таблиц
--

--
-- Индексы таблицы `data_config`
--
ALTER TABLE `data_config`
  ADD PRIMARY KEY (`id_ticker`);

--
-- Индексы таблицы `ticker_map`
--
ALTER TABLE `ticker_map`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `ticker` (`ticker`),
  ADD UNIQUE KEY `symbol` (`symbol`),
  ADD KEY `pair_id` (`pair_id`);

--
-- AUTO_INCREMENT для сохранённых таблиц
--

--
-- AUTO_INCREMENT для таблицы `ticker_map`
--
ALTER TABLE `ticker_map`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=8339;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;