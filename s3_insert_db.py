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

#Postgres settings
user = config['postgres']['username']
password = config['postgres']['password']
host = config['postgres']['host']
port = config['postgres']['port']
dbname = config['postgres']['database']
stage_scheme = config['stage_layer']['scheme_name']

def main():
    # search .csv files in temp folder
    f_paths = s5_common_func.search_by_mask(tmp_folder, '*.csv')     
    if len(f_paths) == 0:
        print('No *.csv files with data found. Run s1 and s2 again.')
        return None

    # create SQLAlchemy engine
    engine = sa.create_engine(f'postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}')
    conn = engine.connect()
    
    # Drop scheme if exists and create new
    answ = input(f'Drop {stage_scheme} schema if exists? (y)')
    if answ == 'y':
        conn.execute(sa.schema.DropSchema(stage_scheme, cascade = True, if_exists = True))
        conn.execute(sa.schema.CreateSchema(stage_scheme))
        conn.commit()
        msg = f'Scheme {stage_scheme} created.'
        print(msg)
        s5_common_func.write_journal(msg) 
    
    # create tables if not exists
    md_obj = sa.MetaData(stage_scheme)
    md_obj = create_tables(md_obj)
    md_obj.create_all(conn, checkfirst = True)  
    conn.commit()
     
    for f_path in f_paths:
        # get short filename (without extension)
        short_name = Path(f_path).stem
        
        # get table name by short filename
        table_name = config['data_model'][short_name]
        
        # get column names  
        col_names = md_obj.tables[stage_scheme + '.' + table_name].columns.keys()
        
        # read data from .csv files
        dataset = pd.read_csv(f_path, delimiter = '|', header = None, names = col_names, encoding = 'utf-8')
        
        # insert dataset into database table        
        result = dataset.to_sql(table_name, conn, stage_scheme, if_exists = 'replace', index = False, chunksize = 1000)
        conn.commit()
        msg = f'{table_name}: inserted'
        print(msg)
        s5_common_func.write_journal(msg)        
      
        # delete .csv files to clean up tmp folder
        os.remove(f_path)
    conn.close()
    
if __name__ == '__main__':
   m = main()