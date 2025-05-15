USE datafeed;

CREATE TABLE `src_activity` (
  `name` varchar(32) NOT NULL,
  `exchange` varchar(16) NOT NULL,
  `host` varchar(32) NOT NULL COMMENT 'for replication',
  `ts_alive` timestamp NOT NULL DEFAULT current_timestamp(),
  `uptime` int(11) NOT NULL DEFAULT 0,
  `errors` int(11) NOT NULL DEFAULT 0,
  `rest_loads` int(11) NOT NULL DEFAULT 0,
  `ws_loads` int(11) NOT NULL DEFAULT 0,
  `PID` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='Add datafeed sources RTM status';