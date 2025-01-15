------------------------------------------------
--функция для загрузки данных в отчет
------------------------------------------------
CREATE OR REPLACE FUNCTION detect_fraud.insert_report()
RETURNS VOID AS $$
BEGIN
    -- Вставка операций при попытке подбора сумм
    INSERT INTO detect_fraud.report (fraud_dt, passport, fio, phone, fraud_type, report_dt)
	    WITH RankedTransactions AS (
	        SELECT passport_num, 
	               last_name,
		       first_name,
		       patronymic,
	               phone,
	               card_num,
	               trans_date,
	               amt,
	               oper_result,
	               ROW_NUMBER() OVER (PARTITION BY card_num ORDER BY trans_date) AS rn
	        FROM detect_fraud.STG_TEMP_TABLE_1
	        WHERE oper_result IN ('Отказ', 'Успешно')
	    ),
	    GroupedTransactions AS (
	        SELECT t1.passport_num,
	               t1.last_name, 
	               t1.first_name, 
	               t1.patronymic, 
	               t1.phone, 
	               t1.trans_date,
	               t1.card_num,
	               t1.oper_result,
	               COUNT(*) AS cnt
	        FROM RankedTransactions t1
	        JOIN RankedTransactions t2 ON t1.card_num = t2.card_num 
	        WHERE t1.rn > t2.rn 
	              AND t1.trans_date BETWEEN t2.trans_date AND t2.trans_date + INTERVAL '20 minutes'
	              AND t1.amt < t2.amt
	        GROUP BY t1.card_num, t1.passport_num, t1.first_name, t1.last_name, t1.patronymic, t1.phone, t1.trans_date, t1.oper_result
	    )
	    SELECT trans_date, passport_num, last_name || ' ' || first_name || ' ' || patronymic, phone, 'Попытка подбора сумм.', now()
	    FROM GroupedTransactions
	    WHERE cnt > 2 and oper_result = 'Успешно';

    -- Вставка операций при просроченном паспорте
    INSERT INTO detect_fraud.report (fraud_dt, passport, fio, phone, fraud_type, report_dt)
	    SELECT trans_date, passport_num, last_name || ' ' || first_name || ' ' || patronymic, phone, 'Совершение операции при просроченном паспорте.', now()
	    FROM detect_fraud.STG_TEMP_TABLE_1
	    WHERE passport_valid_to < now();

    -- Вставка операций при недействующем договоре
    INSERT INTO detect_fraud.report (fraud_dt, passport, fio, phone, fraud_type, report_dt)
	    SELECT trans_date, passport_num, last_name || ' ' || first_name || ' ' || patronymic, phone, 'Совершение операции при недействующем договоре.', now()
	    FROM detect_fraud.STG_TEMP_TABLE_1
	    WHERE valid_to < now();

    -- Вставка операций в разных городах в течение 1 часа
    INSERT INTO detect_fraud.report (fraud_dt, passport, fio, phone, fraud_type, report_dt)
	    SELECT max(t1.trans_date), t1.passport_num, t1.last_name || ' ' || t1.first_name || ' ' || t1.patronymic,
			   t1.phone, 'Совершение операции в разных городах в течение 1 часа.', now()
	    FROM detect_fraud.STG_TEMP_TABLE_1 t1
	    JOIN detect_fraud.STG_TEMP_TABLE_1 t2 
	    ON t1.card_num = t2.card_num 
	       AND t1.trans_id != t2.trans_id 
	       AND t1.terminal_city != t2.terminal_city 
	       AND ABS(EXTRACT(EPOCH FROM (t1.trans_date - t2.trans_date)) / 60) <= 6
	    GROUP BY t1.passport_num, t1.first_name, t1.last_name, t1.patronymic, t1.phone;

END;
$$ LANGUAGE plpgsql;

SELECT detect_fraud.insert_report();
