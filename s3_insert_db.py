# standart modules
import configparser
import sys, os
import pandas as pd
import sqlalchemy as sa
from pathlib import Path

# developed modules
import s5_common_func
from sql.stage_create_objects import create_tables

# read configs
config = configparser.ConfigParser() 
config.read(Path(sys.path[0], 'pipeline.conf'))

tmp_folder = Path(sys.path[0], config['general']['tmp_folder'])

#SQLight settings
sqlite_stage_path = Path(sys.path[0], config['sqlite']['sqlite_stage_file'])

def main():
    # search .csv files in temp folder
    f_paths = s5_common_func.search_by_mask(tmp_folder, '*.csv')     
    if len(f_paths) == 0:
        print('No *.csv files with data found. Run s1 and s2 again.')
        return None

    # create SQLAlchemy engine
    engine = sa.create_engine(f'sqlite:///{sqlite_stage_path}')
    conn = engine.connect()
    md_obj = sa.MetaData()
    md_obj.reflect(conn)
    
    # drop tables if exists
    answ = input(f'{sqlite_stage_path}: \nDROP tables if exists? (y)')
    if answ == 'y':
        md_obj.drop_all(conn)
        conn.commit()

    # create tables
    tables_list = md_obj.sorted_tables
    if len(tables_list) == 0:
        md_obj = create_tables(md_obj)
        md_obj.create_all(conn)  
        conn.commit()
        msg = f'Tables in created.'
        print(msg)
        s5_common_func.write_journal(msg)
    md_obj.reflect(conn)    
    
    for f_path in f_paths:
        # get short filename (without extension)
        short_name = Path(f_path).stem
        
        # get table name by short filename
        table_name = config['data_model'][short_name]
        
        # get column names  
        col_names = md_obj.tables[table_name].columns.keys()
        
        # read data from .csv files
        dataset = pd.read_csv(f_path, delimiter = '|', header = None, names = col_names, encoding = 'utf-8')
        
        # insert dataset into database table        
        result = dataset.to_sql(table_name, conn, if_exists = 'replace', index = False, chunksize = 1000)
        conn.commit()
        msg = f'{table_name}: inserted'
        print(msg)
        s5_common_func.write_journal(msg)        
      
        # delete .csv files to clean up tmp folder
        os.remove(f_path)
    conn.close()
    
if __name__ == '__main__':
    main()