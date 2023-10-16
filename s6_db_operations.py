import sys
import configparser
import psycopg
from pathlib import Path
from math import ceil

# developed modules
import s5_common_func

config = configparser.ConfigParser() 
config.read(Path(sys.path[0], 'pipeline.conf'))

dbname = config['postgres']['database']
user = config['postgres']['username']
password = config['postgres']['password']
host = config['postgres']['host']
port = config['postgres']['port']
        
def connect_to_db():
    try:
        conn = psycopg.connect(
            'dbname=' + dbname
            + ' user=' + user
            + ' password=' + password
            + ' host=' + host,
            port = port)
    except psycopg.OperationalError as e:
        print('psycopg.OperationalError:', e)
        return exit(1)
    return conn

def drop_scheme(sch_name):
    query ='''
            DROP SCHEMA {0} CASCADE;
            '''.format(sch_name)
    a = input(f'Drop scheme "{sch_name}"? (y)')
    if a == 'y':
        conn = connect_to_db()
        curs = conn.cursor()
        curs.execute(query)         
        conn.close()    
    
def select_table_names(sch_name):
    query ='''
            SELECT tablename FROM pg_tables 
            WHERE schemaname = '{0}'
            '''.format(sch_name)
    conn = connect_to_db()
    curs = conn.cursor()
    curs.execute(query)            
    result = curs.execute(query).fetchall()
    conn.close()   
    table_names = [item[0] for item in result]
    return(table_names)  

def select_col_names(table_name):
    query = f'SELECT * FROM {table_name} LIMIT 0;'
    conn = connect_to_db()
    curs = conn.cursor()
    curs.execute(query)
    columns = [desc[0] for desc in curs.description]
    conn.close()
    return columns 

def truncate_table(sch_name, table_name):
    query = f'TRUNCATE {sch_name}.{table_name} CASCADE;'
    conn = connect_to_db()
    curs = conn.cursor()
    curs.execute(query)
    conn.commit()
    conn.close()
        
def insert(table_name, columns, dataset):
    # divide dataset to insert by parts
    data_parts = split_dataset(dataset)

    msg = f'\nINSERT INTO {table_name}:'
    print(msg, end = '')
    s5_common_func.write_journal(msg) 
    
    conn = connect_to_db()
    ins_total = 0
    for part in data_parts:
        try:
            for row in part:
                with conn.cursor() as curs:
                    placeholders = (', ').join(['%s']*len(columns))
                    
                    # "quote" columns to escape lowercase by PostgreSQL
                    cols_str = (', ').join([f'"{name}"' for name in columns])
                    
                    # form set of values in order by columns
                    values = [None if cell == '' else cell for cell in row]

                    query = f'INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders});'
                    curs.execute(query, values)
            conn.commit()
        except psycopg.Error as e:
            print(query)
            print(values)
            print(e)
            #conn.rollback()
            #conn.close()
        
        N = len(part)
        print(f'{N} ', sep=' ', end='', flush=True)
        ins_total = ins_total + N
    conn.close()
    print(f'\nTotal: {ins_total} rows\n')
    
def split_dataset(dataset):
    # divide dataset to insert by parts
    part_len = 1000
    parts = ceil(len(dataset)/part_len)
    return [dataset[part_len*k:part_len*(k+1)] for k in range(parts)]
 
if __name__ == '__main__':
    pass