import sys
import configparser
import psycopg
import sqlite3
from pathlib import Path
from math import ceil

# developed modules
import s5_common_func

config = configparser.ConfigParser() 
config.read(Path(sys.path[0], 'pipeline.conf'))

db_type = config['general']['db_type']

dbname = config['postgres']['database']
user = config['postgres']['username']
password = config['postgres']['password']
host = config['postgres']['host']
port = config['postgres']['port']
        
def connect_to_db(sqlight_path = None):
    try:
        if db_type == 'postgres':
            conn = psycopg.connect(
                'dbname=' + dbname
                + ' user=' + user
                + ' password=' + password
                + ' host=' + host,
                port = port)
                
        elif db_type == 'sqlite':
            conn = sqlite3.connect(sqlight_path)
        else:
            print(f'unknown db_type "{db_type}"')
        
    except psycopg.OperationalError as e:
        print('psycopg.OperationalError:', e)
        conn.close()            
        return exit(1)        
    except sqlite3.DatabaseError as err:
        print('Error: ', err)
        conn.close()
        return exit(1) 
    return conn

def drop_scheme(conn, sch_name):
    query ='''
            DROP SCHEMA {0} CASCADE;
            '''.format(sch_name)
    #conn = connect_to_db()
    curs = conn.cursor()
    curs.execute(query) 
    #?conn.commit()    
    curs.close()
    #conn.close()  
    
def select_table_names(conn, sch_name):
    query ='''
            SELECT tablename FROM pg_tables 
            WHERE schemaname = '{0}'
            '''.format(sch_name)
    #conn = connect_to_db()
    curs = conn.cursor()
    curs.execute(query)            
    result = curs.execute(query).fetchall()
    curs.close()    
    #conn.close()   
    table_names = [item[0] for item in result]
    return(table_names)  

def select_col_names(conn, table_name):
    query = f'SELECT * FROM {table_name} LIMIT 0;'
    #conn = connect_to_db(sqlite_stage_file)
    curs = conn.cursor()
    curs.execute(query)
    columns = [desc[0] for desc in curs.description]
    curs.close()    
    #conn.close()
    return columns 

def truncate_table(conn, table_name):
    if db_type == 'postgres':
        query = f'TRUNCATE {table_name} CASCADE;'
    elif db_type == 'sqlite':
        query = f'DELETE FROM {table_name};'
    print(query)
    #conn = connect_to_db(sqlite_stage_file)
    curs = conn.cursor()
    curs.execute(query)
    curs.close()        
    conn.commit()
    #conn.close()
        
def insert(conn, table_name, columns, dataset):
    # divide dataset to insert by parts
    data_parts = split_dataset(dataset)

    msg = f'\nINSERT INTO {table_name}:'
    print(msg, end = '')
    s5_common_func.write_journal(msg) 
    
    #conn = connect_to_db(sqlite_stage_file)
    curs = conn.cursor()
    ins_total = 0
    for part in data_parts:
        try:
            for row in part:
                if db_type == 'postgres':
                    # form set of values, replace '' cells with None
                    row = [None if cell == '' else cell for cell in row]
                    
                    placeholders = (', ').join(['%s']*len(columns))
                    
                    # "quote" columns to escape lowercase by PostgreSQL
                    cols_str = (', ').join([f'"{name}"' for name in columns]) 
                    
                    query = f'INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders});'
                    curs.execute(query, row)
                elif db_type == 'sqlite':
                    # form set of values, replace None cells with ''
                    row = ['' if cell == None else cell for cell in row]                
                    data_values = (', ').join(f'"{cell}"' for cell in row)
                    cols_str = (', ').join(columns) 
                    query = f'INSERT INTO {table_name} ({cols_str}) VALUES ({data_values});'
                    curs.execute(query)
            conn.commit()
        except psycopg.Error as e:
            print('query:', query)
            print('values:', row)
            print(e)
            conn.rollback()
        except sqlite3.DatabaseError as e:
            print('query:', query)
            print('values:', row)
            print(e)
            conn.rollback()          
   
        N = len(part)
        print(f'{N} ', sep=' ', end='', flush=True)
        ins_total = ins_total + N
    curs.close()
    #conn.close()
    print(f'\nTotal: {ins_total} rows\n')
    
def split_dataset(dataset):
    # divide dataset to insert by parts
    part_len = 1000
    parts = ceil(len(dataset)/part_len)
    return [dataset[part_len*k:part_len*(k+1)] for k in range(parts)]
 
if __name__ == '__main__':
    pass