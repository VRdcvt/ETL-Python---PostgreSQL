import pandas as pd
import psycopg2
from datetime import datetime
from pathlib import Path

# Global values
v_processed_id = 1
v_increment_dt = datetime.now()



# Handling errors when connecting to a database
try:
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='1234',
        host='localhost',
        port='5432'
    )
    conn.autocommit = True
    print(f"Database connection: COMPLETE")
except psycopg2.Error as e:
    print(f"Database connection error with {e}")
    exit()



# creating cursor
try:
    cur = conn.cursor()
    print("Creating cursor: COMPLETE")
except psycopg2.Error as e:
    print(f"Creating cursor error with {e}")
    conn.close()
    exit()



# functions for insert data in meta tables
def insert_meta_etl_function(v_processed_id, v_increment_dt, name_object, status, message):
    insert_table_query = '''
        INSERT INTO detect_fraud.META_ETL (processed_id, processed_dt, name_object, status, message)
        VALUES (%s, %s, %s, %s, %s);
    '''
    cur.execute(insert_table_query, (v_processed_id, v_increment_dt, name_object, status, message))



def insert_meta_files_function(v_processed_id, v_increment_dt, load_date, filename, status, message):
    insert_table_query = '''
        INSERT INTO detect_fraud.META_FILES (processed_id, processed_date, load_date, filename, status, message)
        VALUES (%s, %s, %s, %s, %s, %s);
    '''
    cur.execute(insert_table_query, (v_processed_id, v_increment_dt, load_date, filename, status, message))



def insert_into_STG_TEMP_TABLE_1 (data):
# insert data in temp table
    for index, row in data.iterrows():
        insert_query = '''
        INSERT INTO detect_fraud.STG_TEMP_TABLE_1
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        '''
        # Handling errors when insert into table
        try:
            cur.execute(insert_query, tuple(row) + (v_processed_id,))
        except Exception as e:
            print(f"insert_query table detect_fraud.STG_TEMP_TABLE_1: error with {e}")
            insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.STG_TEMP_TABLE_1', 'error', 'the data has not been uploaded to the table')
            exit()
    print(f"insert_query table detect_fraud.STG_TEMP_TABLE_1: COMPLETE")
    insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.STG_TEMP_TABLE_1', 'load', 'the data has been uploaded to the table')



def insert_into_DIM_CARDS_HIST ():
# FUNCTION detect_fraud.card_scd2_loader2
    function_call_query = '''
    SELECT detect_fraud.card_scd2_loader2(%s, %s);
    '''
    # Handling errors when call function
    try:
        cur.execute(function_call_query, (v_increment_dt, v_processed_id))
        print(f"function_call_query detect_fraud.card_scd2_loader2: COMPLETE")
        insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.DIM_CARDS_HIST', 'load', 'the data has been uploaded to the table')
    except Exception as e:
        print(f"function_call_query detect_fraud.card_scd2_loader2: error with {e}")
        insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.DIM_CARDS_HIST', 'error', 'data has not been uploaded to the table')
        exit()



def insert_into_DIM_CLIENTS_HIST ():
# FUNCTION detect_fraud.client_scd2_loader2
    function_call_query = '''
    SELECT detect_fraud.clients_scd2_loader2(%s, %s);
    '''
    # Handling errors when call function
    try:
        cur.execute(function_call_query, (v_increment_dt, v_processed_id))
        print(f"function_call_query detect_fraud.client_scd2_loader2: COMPLETE")
        insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.DIM_CLIENTS_HIST', 'load', 'the data has been uploaded to the table')
    except Exception as e:
        print(f"function_call_query detect_fraud.client_scd2_loader2: error with {e}")
        insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.DIM_CLIENTS_HIST', 'error', 'data has not been uploaded to the table')
        exit()



def insert_into_DIM_ACCOUNTS_HIST ():
# FUNCTION detect_fraud.accounts_scd2_loader2
    function_call_query = '''
    SELECT detect_fraud.accounts_scd2_loader2(%s, %s);
    '''
    # Handling errors when call function
    try:
        cur.execute(function_call_query, (v_increment_dt, v_processed_id))
        print(f"function_call_query detect_fraud.accounts_scd2_loader2: COMPLETE")
        insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.DIM_ACCOUNTS_HIST', 'load', 'the data has been uploaded to the table')
    except Exception as e:
        print(f"function_call_query detect_fraud.accounts_scd2_loader2: error with {e}")
        insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.DIM_ACCOUNTS_HIST', 'error', 'data has not been uploaded to the table')
        exit()



def insert_into_DIM_TERMINALS_HIST ():
# FUNCTION detect_fraud.terminals_scd2_loader2
    function_call_query = '''
    SELECT detect_fraud.terminals_scd2_loader2(%s, %s);
    '''
    # Handling errors when call function
    try:
        cur.execute(function_call_query, (v_increment_dt, v_processed_id))
        print(f"function_call_query detect_fraud.terminals_scd2_loader2: COMPLETE")
        insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.DIM_TERMINALS_HIST', 'load', 'the data has been uploaded to the table')
    except Exception as e:
        print(f"function_call_query detect_fraud.terminals_scd2_loader2: error with {e}")
        insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.DIM_TERMINALS_HIST', 'error', 'data has not been uploaded to the table')
        exit()



def insert_into_FACT_TRANSACTIONS ():
# FUNCTION detect_fraud.fact_transactions_scd1_loader2
    function_call_query = '''
    SELECT detect_fraud.fact_transactions_scd1_loader2(%s, %s);
    '''
    # Handling errors when call function
    try:
        cur.execute(function_call_query, (v_increment_dt, v_processed_id))
        print(f"function_call_query detect_fraud.fact_transactions_scd1_loader2: COMPLETE")
        insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.FACT_TRANSACTIONS', 'load', 'the data has been uploaded to the table')
    except Exception as e:
        print(f"function_call_query detect_fraud.fact_transactions_scd1_loader2: error with {e}")
        insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.FACT_TRANSACTIONS', 'error', 'data has not been uploaded to the table')
        exit()



def insert_into_REPORT ():
# FUNCTION detect_fraud.insert_report
    function_call_query = '''
    SELECT detect_fraud.insert_report();
    '''
    # Handling errors when call function
    try:
        cur.execute(function_call_query)
        print(f"function_call_query detect_fraud.insert_report: COMPLETE")
        insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.REPORT', 'load', 'the data has been uploaded to the table')
    except Exception as e:
        print(f"function_call_query detect_fraud.insert_report: error with {e}")
        insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.REPORT', 'error', 'data has not been uploaded to the table')
        exit()



def main():
    # Read script where wich creations all tables
    with open(r'ПУТЬ К ФАЙЛУ DDL.sql', 'r') as file:  # ВАЖНО УКАЗАТЬ ПУТЬ К ФАЙЛУ DDL.sql
        sql_script = file.read()
    # Handling errors when create tables
    try:
        cur.execute(sql_script)
        print('Create table META_FILES: COMPLETE')
        print('Create table META_ETL: COMPLETE')
        print('Create table STG_TEMP_TABLE_1: COMPLETE')
        print('Create table DIM_CARDS_HIST: COMPLETE')
        print('Create table DIM_CLIENTS_HIST: COMPLETE')
        print('Create table DIM_ACCOUNTS_HIST: COMPLETE')
        print('Create table DIM_TERMINALS_HIST: COMPLETE')
        print('Create table FACT_TRANSACTIONS: COMPLETE')
        print('Create table REPORT: COMPLETE')
        
    except Exception as e:
        print(f"Error with read script: {e}")
        exit()



    # Download data from excell
    excel_file = f'C:\\Users\\Vitaly\\Desktop\\fraud_detection\\transactions_0{v_processed_id}052020.xlsx' # ВАЖНО УКАЗАТЬ ПУТЬ К ФАЙЛУ transactions_01052020.xlsx, вместо цифры "1" пишем {v_processed_id}
    sheet_name = f'0{v_processed_id}052020'

    full_path = Path(rf'C:\Users\Vitaly\Desktop\fraud_detection\transactions_0{v_processed_id}052020.xlsx') # ВАЖНО УКАЗАТЬ ПУТЬ К ФАЙЛУ transactions_01052020.xlsx, вместо цифры "1" пишем {v_processed_id}
    file_name = full_path.name

    # Handling errors when reading an Excel file
    try:
        data = pd.read_excel(excel_file, sheet_name=sheet_name)
        insert_meta_files_function(v_processed_id, v_increment_dt, datetime.now(), file_name, 'load', 'file completely load')

    except FileNotFoundError:
        print(f"Error: file '{file_name}' not found.")
        insert_meta_files_function(v_processed_id, v_increment_dt, datetime.now(), file_name, 'error', 'file not found.')
        exit()

    except Exception as e:
        print(f"Error with reading Excel file: {e}")
        insert_meta_files_function(v_processed_id, v_increment_dt, datetime.now(), file_name, 'error', 'file not reading.')
        exit()

   
    # Download meta files in meta tables
    insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.META_FILES', 'load', 'table has benn created')
    insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.META_ETL', 'load', 'table has benn created')
    insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.STG_TEMP_TABLE_1', 'load', 'table has benn created')
    insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.DIM_CARDS_HIST', 'load', 'table has benn created')
    insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.DIM_CLIENTS_HIST', 'load', 'table has benn created')
    insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.DIM_ACCOUNTS_HIST', 'load', 'table has benn created')
    insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.DIM_TERMINALS_HIST', 'load', 'table has benn created')
    insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.FACT_TRANSACTIONS', 'load', 'table has benn created')
    insert_meta_etl_function(v_processed_id, v_increment_dt, 'detect_fraud.REPORT', 'load', 'table has benn created') 


    # Insert data in tables
    insert_into_STG_TEMP_TABLE_1(data)
    insert_into_DIM_CARDS_HIST()
    insert_into_DIM_CLIENTS_HIST()
    insert_into_DIM_ACCOUNTS_HIST()
    insert_into_DIM_TERMINALS_HIST()
    insert_into_FACT_TRANSACTIONS()
    insert_into_REPORT()


    # save changes and close connection
    conn.commit()
    cur.close()
    conn.close()



if __name__ == "__main__":
    main()
