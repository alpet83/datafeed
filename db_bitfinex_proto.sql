-- phpMyAdmin SQL Dump
-- version 5.2.1deb3
-- https://www.phpmyadmin.net/
--
-- Хост: localhost:3306
-- Время создания: Апр 21 2025 г., 13:40
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
-- База данных: `bitfinex`
--
CREATE DATABASE IF NOT EXISTS `bitfinex` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE `bitfinex`;

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
(1, 1, 1, 3),
(2, 1, 1, 3),
(3, 1, 1, 3),
(4, 1, 1, 3),
(5, 1, 1, 3),
(6, 1, 1, 3),
(7, 1, 1, 3),
(8, 1, 1, 3),
(9, 1, 1, 3),
(10, 1, 1, 3),
(11, 1, 1, 3),
(12, 1, 1, 3),
(13, 1, 0, 3),
(14, 1, 1, 3),
(15, 1, 1, 3),
(16, 1, 1, 3),
(17, 1, 1, 3),
(18, 1, 1, 3),
(19, 1, 1, 3),
(20, 1, 1, 3),
(21, 1, 1, 3),
(22, 1, 0, 3),
(23, 1, 0, 3),
(24, 1, 0, 3),
(25, 1, 0, 3),
(26, 1, 0, 3),
(27, 0, 0, 3),
(28, 1, 0, 3),
(29, 3, 0, 3),
(31, 3, 0, 3);

-- --------------------------------------------------------

--
-- Структура таблицы `market_track`
--

CREATE TABLE `market_track` (
  `ts` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `ticker` varchar(16) NOT NULL,
  `period` varchar(3) NOT NULL,
  `value_type` varchar(16) NOT NULL,
  `value_name` varchar(16) NOT NULL,
  `value` double NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin;

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
(1, 'adausd', 'tADAUSD', 40),
(2, 'algusd', 'tALGUSD', 6),
(3, 'atousd', 'tATOUSD', 7),
(4, 'avaxusd', 'tAVAX:USD', 17),
(5, 'btcusd', 'tBTCUSD', 1),
(6, 'dogeusd', 'tDOGE:USD', 50),
(7, 'dotusd', 'tDOTUSD', 51),
(8, 'ethusd', 'tETHUSD', 3),
(9, 'filusd', 'tFILUSD', 16),
(10, 'icpusd', 'tICPUSD', 52),
(11, 'linkusd', 'tLINK:USD', 19),
(12, 'ltcusd', 'tLTCUSD', 4),
(13, 'dashusd', 'tDSHUSD', 13),
(14, 'maticusd', 'tMATIC:USD', 37),
(15, 'nearusd', 'tNEAR:USD', 25),
(16, 'solusd', 'tSOLUSD', 54),
(17, 'trxusd', 'tTRXUSD', 64),
(18, 'uniusd', 'tUNIUSD', 27),
(19, 'xlmusd', 'tXLMUSD', 28),
(20, 'xrpusd', 'tXRPUSD', 22),
(21, 'bchusd', 'tBCHN:USD', 9),
(22, 'aptusd', 'tAPTUSD', 8),
(23, 'arbusd', 'tARBUSD', 18),
(24, 'etcusd', 'tETCUSD', 15),
(25, 'ethbtc', 'tETHBTC', 112),
(26, 'pepeusd', 'tPEPE:USD', 76),
(27, '1inchusd', 't1INCH:USD', 41),
(28, 'dashbtc', 'tDSHBTC', 114),
(29, 'xautusd', 'tXAUT:USD', 2),
(31, 'btceur', 'tBTCEUR', 91);

--
-- Индексы сохранённых таблиц
--

--
-- Индексы таблицы `data_config`
--
ALTER TABLE `data_config`
  ADD PRIMARY KEY (`id_ticker`);

--
-- Индексы таблицы `market_track`
--
ALTER TABLE `market_track`
  ADD UNIQUE KEY `complex` (`ticker`,`value_type`,`value_name`,`period`) USING BTREE,
  ADD KEY `ticker` (`ticker`),
  ADD KEY `period` (`period`),
  ADD KEY `value_type` (`value_type`),
  ADD KEY `value_name` (`value_name`);

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
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=1988;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
