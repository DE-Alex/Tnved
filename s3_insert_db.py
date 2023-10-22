# standart modules
import configparser
import sys, os
import csv
import glob
from pathlib import Path

# developed modules
import s5_common_func
import s6_db_operations
from sql.stage_create_objects import create_scheme
from sql.stage_create_objects import create_tables

# read configs
config = configparser.ConfigParser() 
config.read(Path(sys.path[0], 'pipeline.conf'))

tmp_folder = Path(sys.path[0], config['general']['tmp_folder'])
db_type = config['general']['db_type']
stage_scheme = config['stage_layer']['scheme_name']

file_name = config['sqlite']['sqlite_stage_file']
sqlite_stage_path = Path(sys.path[0], file_name)


def main():
    # search .csv files in temp folder
    f_paths = s5_common_func.search_by_mask(tmp_folder, '*.csv')     
    if len(f_paths) == 0:
        print('No *.csv files with data found. Run s1 and s2 again.')
        return None

    # @@@@@remove after testing@@@@@
    if db_type == 'sqlite':
        conn = s6_db_operations.connect_to_db(sqlite_stage_path)
        pass
    elif db_type == 'postgres':
        conn = s6_db_operations.connect_to_db()
        a = input(f'Drop scheme "{stage_scheme}"? (y)')
        if a == 'y':
            s6_db_operations.drop_scheme(conn, stage_scheme)
            print(f'scheme {stage_scheme} droped')
            # create scheme 
            curs = conn.cursor()
            for query in create_scheme:
                curs.execute(query)
            curs.close()
            print(f'scheme {stage_scheme} created')
    # @@@@@remove after testing@@@@@            
            
    # check db tables and create if not exist 
    curs = conn.cursor()    
    for query in create_tables:
        #curs = conn.cursor()
        curs.execute(query)
    curs.close()
    conn.commit()
    #conn.close()
    msg = f'"CREATE <table> IF NOT EXIST" - ok'
    print(msg)
    s5_common_func.write_journal(msg)
  
    # read data from .csv files
    for f_path in f_paths:
        # get short filename (without extension)
        short_name = Path(f_path).stem
        
        # get table name by short filename
        table_name = config['stage_layer'][short_name]
        
        if db_type == 'postgres':
            table_name = f'{stage_scheme}.{table_name}'
        
        # truncate table to clean it if data exists
        s6_db_operations.truncate_table(conn, table_name)    
    
        with open(f_path, newline='') as csv_file:
            dt_reader = csv.reader(csv_file, delimiter = '|')
            dataset = list(dt_reader)
      
        # get column names from table
        columns = s6_db_operations.select_col_names(conn, table_name)
        # insert dataset into table
        s6_db_operations.insert(conn, table_name, columns, dataset)
                
        # delete .csv files to clean up tmp folder
        os.remove(f_path)
    
    conn.close()
    
if __name__ == '__main__':
    main()
    
