# standart modules
import configparser
import sys, os
import csv
import glob
from pathlib import Path

# developed modules
import s4_db_operations
import s5_common_func
import sql.create_objects

# read configs
config = configparser.ConfigParser() 
config.read(Path(sys.path[0], 'pipeline.conf'))

tmp_folder = Path(sys.path[0], config['general']['tmp_folder'])
sch_name = config['db_scheme']['scheme_name']

def main():
    s4_db_operations.drop_scheme(sch_name)#@@@@@@@@@@
 
    # create tables if not exist  
    conn = s4_db_operations.connect_to_db()    
    for query in sql.create_objects.actions:
        print(query)
        curs = conn.cursor()
        curs.execute(query)
        conn.commit()
        print('ok')
    conn.close()
    msg = f'Create tables (if not exist) - ok'
    print(msg)
    s5_common_func.write_journal(msg)

    # select table names from db
    table_names = s4_db_operations.select_table_names(sch_name)
    
    # truncate tables with data if already exist
    for tb_name in table_names:
        s4_db_operations.truncate_table(sch_name, tb_name)
        msg = f'TRUNCATE {tb_name} - ok'
        print(msg)
        s5_common_func.write_journal(msg)
 
    # search .csv files in temp folder
    f_paths = s5_common_func.search_by_mask(tmp_folder, '*.csv')
    if len(f_paths) == 0:
        print('\n - *.csv fles with data not found. \n - Extract and process them again with "s1" and "s2" scripts.')
    
    # read data from .csv files
    for f_path in f_paths:
        with open(f_path, newline='') as csv_file:
            dt_reader = csv.reader(csv_file, delimiter = '|')
            dataset = list(dt_reader)
        
        # get short filename (without extension)
        short_name = Path(f_path).stem
        # get table name by short filename
        table_name = sch_name + '.' + config['db_scheme'][short_name]
        
        # get column names from table
        columns = s4_db_operations.select_col_names(table_name)
        # insert dataset into table
        s4_db_operations.insert(table_name, columns, dataset)
                
        # delete .csv files to clean up tmp folder
        os.remove(f_path)

if __name__ == '__main__':
    main()