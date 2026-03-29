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
(1, 1, 0, 3),
(2, 1, 0, 0),
(5, 1, 0, 0),
(7, 1, 0, 0),
(8, 1, 0, 0),
(12, 1, 0, 3),
(16, 1, 0, 0),
(17, 1, 0, 0),
(23, 1, 0, 0),
(31, 1, 0, 3);

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
(5, 'avaxusd', 'AVAXUSD', 17),
(7, 'bchusd', 'BCHUSD', 9),
(8, 'dogeusd', 'DOGEUSD', 50),
(12, 'ethusd', 'ETHUSD', 3),
(16, 'linkusd', 'LINKUSD', 19),
(17, 'ltcusd', 'LTCUSD', 4),
(23, 'solusd', 'SOLUSD', 54),
(31, 'xrpusd', 'XRPUSD', 22);

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