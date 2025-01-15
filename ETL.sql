------------------------------------------------
--загрузчик SCD2 для таблицы dim_cards_hist
------------------------------------------------
CREATE OR REPLACE FUNCTION detect_fraud.card_scd2_loader2(
  v_increment_dt timestamp,
  v_processed_id int
) 
RETURNS integer AS $$
DECLARE
   result_code integer := 0;
BEGIN
  -- Меняем end_dt в таблице dim_cards_hist только в тех строках, где совпадают все значения строк из таблицы stg_temp_table_1
  --и только в записях с предыдущим processed_id
  UPDATE detect_fraud.dim_cards_hist
  SET end_dt = v_increment_dt
  WHERE card_num IN (
    SELECT dim.card_num
    FROM detect_fraud.dim_cards_hist AS dim
    JOIN detect_fraud.stg_temp_table_1 AS stg ON stg.card_num = dim.card_num
      AND stg.account_num = dim.account_num
  )
	AND detect_fraud.dim_cards_hist.processed_id = v_processed_id - 1;

  -- Добавляем в таблицу dim_cards_hist строки, которые добавляются с новой версии файла из таблицы stg_temp_table_1
  -- Присваиваем также processed_id в соответствии с новым из stg_temp_table_1
  -- start_dt принимается аргументом функции
  INSERT INTO detect_fraud.dim_cards_hist (card_num, account_num, start_dt, processed_id)
  SELECT DISTINCT 
    card_num,
    account_num,
    v_increment_dt,
    v_processed_id
  FROM 
    detect_fraud.stg_temp_table_1 AS stg;

  RETURN result_code;

END;
$$ LANGUAGE 'plpgsql';  
	



------------------------------------------------
--загрузчик SCD2 для таблицы dim_clients_hist
------------------------------------------------
CREATE OR REPLACE FUNCTION detect_fraud.clients_scd2_loader2(
  v_increment_dt timestamp,
  v_processed_id int
) 
RETURNS integer AS $$
DECLARE
   result_code integer := 0;
BEGIN
    -- Меняем end_dt в таблице dim_cards_hist только в тех строках, где совпадают все значения строк из таблицы stg_temp_table_1
    --и только в записях с предыдущим processed_id
  UPDATE detect_fraud.dim_clients_hist
  SET end_dt = v_increment_dt
  WHERE client_id IN (
    SELECT dim.client_id
    FROM detect_fraud.dim_clients_hist AS dim
    JOIN detect_fraud.stg_temp_table_1 AS stg ON stg.client = dim.client_id
        AND stg.last_name = dim.last_name
		AND stg.first_name = dim.first_name
		AND stg.patronymic = dim.patronymic
		AND stg.date_of_birth = dim.date_of_birth
		AND stg.passport_num = dim.passport_num
		AND stg.passport_valid_to = dim.passport_valid_to
		AND stg.phone = dim.phone
        AND dim.processed_id = stg.processed_id - 1
	 )
	AND detect_fraud.dim_clients_hist.processed_id = v_processed_id - 1;

  -- Добавляем в таблицу dim_clients_hist строки, которые добавляются с новой версии файла из таблицы stg_temp_table_1
  -- Присваиваем также processed_id в соответствии с новым из stg_temp_table_1
  -- start_dt принимается аргументом функции
  INSERT INTO detect_fraud.dim_clients_hist (client_id, last_name, first_name, patronymic, date_of_birth,
											 passport_num, passport_valid_to, phone, start_dt, processed_id)
  SELECT DISTINCT 
    client,
    last_name,
    first_name,
    patronymic,
	date_of_birth, 
	passport_num, 
	passport_valid_to, 
	phone,
    v_increment_dt,
    v_processed_id
  FROM 
    detect_fraud.stg_temp_table_1 AS stg;

  RETURN result_code;

END;
$$ LANGUAGE 'plpgsql'; 
	



------------------------------------------------
--загрузчик SCD2 для таблицы dim_accounts_hist
------------------------------------------------
CREATE OR REPLACE FUNCTION detect_fraud.accounts_scd2_loader2(
  v_increment_dt timestamp,
  v_processed_id int
) 
RETURNS integer AS $$
DECLARE
   result_code integer := 0;
BEGIN
    -- Меняем end_dt в таблице dim_accounts_hist только в тех строках, где совпадают все значения строк из таблицы stg_temp_table_1
    --и только в записях с предыдущим processed_id
  UPDATE detect_fraud.dim_accounts_hist
  SET end_dt = v_increment_dt
  WHERE account_num IN (
    SELECT dim.account_num
    FROM detect_fraud.dim_accounts_hist AS dim
    JOIN detect_fraud.stg_temp_table_1 AS stg ON stg.account_num = dim.account_num
        AND stg.valid_to = dim.valid_to
		AND stg.client = dim.client
	 )
	AND detect_fraud.dim_accounts_hist.processed_id = v_processed_id - 1;

  -- Добавляем в таблицу dim_accounts_hist строки, которые добавляются с новой версии файла из таблицы stg_temp_table_1
  -- Присваиваем также processed_id в соответствии с новым из stg_temp_table_1
  -- start_dt принимается аргументом функции
  INSERT INTO detect_fraud.dim_accounts_hist (account_num, valid_to, client, start_dt, processed_id)
  SELECT DISTINCT 
    account_num,
    valid_to,
    client,
    v_increment_dt,
    v_processed_id
  FROM 
    detect_fraud.stg_temp_table_1 AS stg;

  RETURN result_code;

END;
$$ LANGUAGE 'plpgsql'; 





------------------------------------------------
--загрузчик SCD2 для таблицы dim_terminals_hist
------------------------------------------------
CREATE OR REPLACE FUNCTION detect_fraud.terminals_scd2_loader2(
  v_increment_dt timestamp,
  v_processed_id int
) 
RETURNS integer AS $$
DECLARE
   result_code integer := 0;
BEGIN
    -- Меняем end_dt в таблице dim_terminals_hist только в тех строках, где совпадают все значения строк из таблицы stg_temp_table_1
    --и только в записях с предыдущим processed_id
  UPDATE detect_fraud.dim_terminals_hist
  SET end_dt = v_increment_dt
  WHERE terminal_id IN (
    SELECT dim.terminal_id
    FROM detect_fraud.dim_terminals_hist AS dim
    JOIN detect_fraud.stg_temp_table_1 AS stg ON stg.terminal = dim.terminal_id
        AND stg.terminal_type = dim.terminal_type
		AND stg.terminal_city = dim.terminal_city
		AND stg.terminal_adress = dim.terminal_adress
	 )
	AND detect_fraud.dim_terminals_hist.processed_id = v_processed_id - 1;

  -- Добавляем в таблицу dim_terminals_hist строки, которые добавляются с новой версии файла из таблицы stg_temp_table_1
  -- Присваиваем также processed_id в соответствии с новым из stg_temp_table_1
  -- start_dt принимается аргументом функции
  INSERT INTO detect_fraud.dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_adress, start_dt, processed_id)
  SELECT DISTINCT 
    terminal,
    terminal_type,
    terminal_city,
	terminal_adress,
    v_increment_dt,
    v_processed_id
  FROM 
    detect_fraud.stg_temp_table_1 AS stg;

  RETURN result_code;

END;
$$ LANGUAGE 'plpgsql'; 






------------------------------------------------
--загрузчик SCD1 для таблицы fact_transactions
------------------------------------------------
CREATE OR REPLACE FUNCTION detect_fraud.fact_transactions_scd1_loader2(
  v_increment_dt timestamp,
  v_processed_id int
) 
RETURNS integer AS $$
DECLARE
   result_code integer := 0;
BEGIN
    
    -- Обновляем записи
    UPDATE detect_fraud.fact_transactions
    SET update_dt = v_increment_dt,
        processed_id = v_processed_id
    WHERE processed_id != v_processed_id; 

    -- Проверяем, были ли уже вставлены записи
    IF NOT EXISTS (
        SELECT 1 
        FROM detect_fraud.fact_transactions 
        WHERE processed_id = v_processed_id
    ) THEN
        -- Если записи с данным processed_id не существуют, выполняем вставку
        INSERT INTO detect_fraud.fact_transactions (trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal, processed_id)
        SELECT trans_id,
               trans_date,
               card_num,
               oper_type,
               amt,
               oper_result,
               terminal,
               v_processed_id
        FROM detect_fraud.stg_temp_table_1 AS stg;

    END IF;

    RETURN result_code;

END;
$$ LANGUAGE 'plpgsql';
