drop table IF EXISTS detect_fraud.stg_temp_table_1 cascade;
drop table IF EXISTS detect_fraud.fact_transactions cascade;
drop table IF EXISTS detect_fraud.dim_cards_hist cascade;
drop table IF EXISTS detect_fraud.dim_terminals_hist cascade;
drop table IF EXISTS detect_fraud.dim_clients_hist cascade;
drop table IF EXISTS detect_fraud.dim_accounts_hist cascade;
drop table IF EXISTS detect_fraud.meta_files cascade;
drop table IF EXISTS detect_fraud.meta_etl cascade;
drop table IF EXISTS detect_fraud.report cascade;

--таблица с метаданными о загрузке файлов
CREATE TABLE IF NOT EXISTS detect_fraud.META_FILES (
    processed_id           INT,
    processed_date         TIMESTAMP,
    load_date              TIMESTAMP,
    filename               VARCHAR(100),
    status                 VARCHAR(5),
    message                TEXT
);

--таблица с метаданными о добавлении данных
CREATE TABLE IF NOT EXISTS detect_fraud.META_ETL (
    processed_id        INT,
    processed_dt        TIMESTAMP,
    name_object         VARCHAR(100),
    status              VARCHAR(5),
    message             TEXT
);

--общая таблица (все данные из файла)
CREATE TABLE IF NOT EXISTS detect_fraud.STG_TEMP_TABLE_1 (
    trans_id             VARCHAR(100),
    trans_date           timestamp,
    card_num             VARCHAR(100),
    account_num          VARCHAR(100),
    valid_to             DATE,
    client               VARCHAR(100),
    last_name            VARCHAR(100),
    first_name           VARCHAR(100),
    patronymic           VARCHAR(100),
    date_of_birth        DATE,
    passport_num         VARCHAR(100),
    passport_valid_to    DATE,
    phone                VARCHAR(100),
    oper_type            VARCHAR(100),
    amt                  DECIMAL,
    oper_result          VARCHAR(100),
    terminal             VARCHAR(100),
    terminal_type        VARCHAR(100),
    terminal_city        VARCHAR(100),
    terminal_adress      VARCHAR(100),
    processed_id         INT
);


--таблица с картами
CREATE TABLE IF NOT EXISTS detect_fraud.DIM_CARDS_HIST (
    card_surrogate_key     SERIAL,
    card_num               VARCHAR(100),
    account_num            VARCHAR(100),
    start_dt               TIMESTAMP DEFAULT NOW(),
    end_dt                 TIMESTAMP DEFAULT '5999-01-01 00:00:00',
    processed_id           INT,
    CONSTRAINT PK_card_sk  PRIMARY KEY(card_surrogate_key)
);


--таблица клиентов
CREATE TABLE IF NOT EXISTS detect_fraud.DIM_CLIENTS_HIST (
    client_surrogate_key        SERIAL,
    client_id                   VARCHAR(100),
    last_name                   VARCHAR(100),
    first_name                  VARCHAR(100),
    patronymic                  VARCHAR(100),
    date_of_birth               DATE,
    passport_num                VARCHAR(100),
    passport_valid_to           DATE,
    phone                       VARCHAR(100),
    start_dt                    TIMESTAMP DEFAULT NOW(),
    end_dt                      TIMESTAMP DEFAULT '5999-01-01 00:00:00',
    processed_id                INT,
    CONSTRAINT PK_client_sk     PRIMARY KEY(client_surrogate_key)
);


--таблица аккаунтов
CREATE TABLE IF NOT EXISTS detect_fraud.DIM_ACCOUNTS_HIST (
    account_surrogate_key    SERIAL,
    account_num              VARCHAR(100),
    valid_to                 DATE,
    client                   VARCHAR(100),
    start_dt                 TIMESTAMP DEFAULT NOW(),
    end_dt                   TIMESTAMP DEFAULT '5999-01-01 00:00:00',
    processed_id             INT,
    CONSTRAINT PK_account_sk PRIMARY KEY(account_surrogate_key)
);


--таблица терминатов
CREATE TABLE IF NOT EXISTS detect_fraud.DIM_TERMINALS_HIST (
    terminal_surrogate_key    SERIAL,
    terminal_id               VARCHAR(100),
    terminal_type             VARCHAR(100),
    terminal_city             VARCHAR(100),
    terminal_adress           VARCHAR(100),
    start_dt                  TIMESTAMP DEFAULT NOW(),
    end_dt                    TIMESTAMP DEFAULT '5999-01-01 00:00:00',
    processed_id              INT,
    CONSTRAINT PK_terminal_sk PRIMARY KEY(terminal_surrogate_key)
);


--таблица транзакций
CREATE TABLE IF NOT EXISTS detect_fraud.fact_transactions (
    terminal_surrogate_key SERIAL,
    trans_id               VARCHAR(100),
    trans_date             TIMESTAMP,
    card_num               VARCHAR(100),
    oper_type              VARCHAR(100),
    amt                    DECIMAL,
    oper_result            VARCHAR(100),
    terminal               VARCHAR(100),
    create_dt              TIMESTAMP DEFAULT NOW(),
    update_dt              TIMESTAMP DEFAULT NULL,
    processed_id           INT,
    CONSTRAINT PK_trans_sk PRIMARY KEY(terminal_surrogate_key)
);


--таблица с отчетом
CREATE TABLE IF NOT EXISTS detect_fraud.REPORT (
    fraud_dt               TIMESTAMP,
    passport               VARCHAR(100),
    fio                    VARCHAR(100),
    phone                  VARCHAR(100),
    fraud_type             VARCHAR(100),
    report_dt              TIMESTAMP
);



