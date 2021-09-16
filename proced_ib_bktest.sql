USE ib_bktest;

DROP PROCEDURE IF EXISTS accum_data;

DELIMITER //

CREATE PROCEDURE accum_data ()
BEGIN
	SPX_5mins: BEGIN
		DECLARE datevalue_pre DATETIME DEFAULT '2021-01-01 00:00:00';
        SELECT datetime INTO datevalue_pre
        FROM ib_bktest.Data_SPX_5mins 
        ORDER BY datetime DESC LIMIT 1;
		INSERT INTO ib_bktest.Data_SPX_5mins
		SELECT * FROM ib_trade.Data_SPX_5mins
		WHERE datetime > datevalue_pre;
	END SPX_5mins;
	VIX_5mins: BEGIN
		DECLARE datevalue_pre DATETIME DEFAULT '2021-01-01 00:00:00';
        SELECT datetime INTO datevalue_pre
        FROM ib_bktest.Data_VIX_5mins
        ORDER BY datetime DESC LIMIT 1;
		INSERT INTO ib_bktest.Data_VIX_5mins
		SELECT * FROM ib_trade.Data_VIX_5mins
		WHERE datetime > datevalue_pre;
	END VIX_5mins;
	VVIX_5mins: BEGIN
		DECLARE datevalue_pre DATETIME DEFAULT '2021-01-01 00:00:00';
        SELECT datetime INTO datevalue_pre
        FROM ib_bktest.Data_VVIX_5mins
        ORDER BY datetime DESC LIMIT 1;
		INSERT INTO ib_bktest.Data_VVIX_5mins
		SELECT * FROM ib_trade.Data_VVIX_5mins
		WHERE datetime > datevalue_pre;
	END VVIX_5mins;
	NAQ_5mins: BEGIN
		DECLARE datevalue_pre DATETIME DEFAULT '2021-01-01 00:00:00';
        SELECT datetime INTO datevalue_pre
        FROM ib_bktest.Data_NAQ_5mins
        ORDER BY datetime DESC LIMIT 1;
		INSERT INTO ib_bktest.Data_NAQ_5mins
		SELECT * FROM ib_trade.Data_NAQ_5mins
		WHERE datetime > datevalue_pre;
	END NAQ_5mins;
	SPX_1day: BEGIN
		DECLARE datevalue_pre DATETIME DEFAULT '2021-01-01 00:00:00';
        SELECT date INTO datevalue_pre
        FROM ib_bktest.Data_SPX_1day
        ORDER BY date DESC LIMIT 1;
		INSERT INTO ib_bktest.Data_SPX_1day
		SELECT * FROM ib_trade.Data_SPX_1day
		WHERE date > datevalue_pre;
	END SPX_1day;
	VIX_1day: BEGIN
		DECLARE datevalue_pre DATETIME DEFAULT '2021-01-01 00:00:00';
        SELECT date INTO datevalue_pre
        FROM ib_bktest.Data_VIX_1day
        ORDER BY date DESC LIMIT 1;
		INSERT INTO ib_bktest.Data_VIX_1day
		SELECT * FROM ib_trade.Data_VIX_1day
		WHERE date > datevalue_pre;
	END VIX_1day;
	VVIX_1day: BEGIN
		DECLARE datevalue_pre DATETIME DEFAULT '2021-01-01 00:00:00';
        SELECT date INTO datevalue_pre
        FROM ib_bktest.Data_VVIX_1day
        ORDER BY date DESC LIMIT 1;
		INSERT INTO ib_bktest.Data_VVIX_1day
		SELECT * FROM ib_trade.Data_VVIX_1day
		WHERE date > datevalue_pre;
	END VVIX_1day;
	NAQ_1day: BEGIN
		DECLARE datevalue_pre DATETIME DEFAULT '2021-01-01 00:00:00';
        SELECT date INTO datevalue_pre
        FROM ib_bktest.Data_NAQ_1day
        ORDER BY date DESC LIMIT 1;
		INSERT INTO ib_bktest.Data_NAQ_1day
		SELECT * FROM ib_trade.Data_NAQ_1day
		WHERE date > datevalue_pre;
	END NAQ_1day;
	ind_datas: BEGIN
		DECLARE datevalue_pre DATETIME DEFAULT '2021-01-01 00:00:00';
        SELECT datetime INTO datevalue_pre
        FROM ib_bktest.ind_datas
        ORDER BY datetime DESC LIMIT 1;
		INSERT INTO ib_bktest.ind_datas
		SELECT * FROM ib_trade.ind_datas
		WHERE datetime > datevalue_pre;
	END ind_datas;
	trade_log: BEGIN
		DECLARE datevalue_pre DATETIME DEFAULT '2021-01-01 00:00:00';
        SELECT datetime INTO datevalue_pre
        FROM ib_bktest.trade_log
        ORDER BY datetime DESC LIMIT 1;
		INSERT INTO ib_bktest.trade_log
		SELECT * FROM ib_trade.trade_log
		WHERE datetime > datevalue_pre;
	END trade_log;
	perf_datas: BEGIN
		DECLARE datevalue_pre DATETIME DEFAULT '2021-01-01 00:00:00';
        SELECT datetime INTO datevalue_pre
        FROM ib_bktest.perf_datas
        ORDER BY datetime DESC LIMIT 1;
		INSERT INTO ib_bktest.perf_datas
		SELECT * FROM ib_trade.perf_datas
		WHERE datetime > datevalue_pre;
	END perf_datas;
	mk_signal: BEGIN
		DECLARE datevalue_pre DATETIME DEFAULT '2021-01-01 00:00:00';
        SELECT datetime INTO datevalue_pre
        FROM ib_bktest.mk_signal
        ORDER BY datetime DESC LIMIT 1;
		INSERT INTO ib_bktest.mk_signal
		SELECT * FROM ib_trade.mk_signal
		WHERE datetime > datevalue_pre;
	END mk_signal;
END //
DELIMITER ;