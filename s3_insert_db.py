# standart modules
import configparser
import sys, os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from pathlib import Path

# developed modules
import s5_common_func
from sql.stage_create_objects import create_tables

# read configs
config = configparser.ConfigParser() 
config.read(Path(sys.path[0], 'pipeline.conf'))

tmp_folder = Path(sys.path[0], config['general']['tmp_folder'])
db_type = config['general']['db_type']

#SQLight settings
sqlite_stage_path = Path(sys.path[0], config['sqlite']['sqlite_stage_file'])

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
    if db_type == 'sqlite':
        engine = create_engine(f'sqlite:///{sqlite_stage_path}')
        db_schema = None
    elif db_type == 'postgres':
        engine = create_engine(f'postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}')
        db_schema = stage_scheme
    conn = engine.connect()
    md_obj = MetaData(db_schema)    
    
    a = input(f'Drop scheme "{stage_scheme}"? (y)')
    if a == 'y':
        md_obj.reflect(conn)
        md_obj.drop_all(conn)
        md_obj = MetaData(db_schema)
        md_obj = create_tables(md_obj)
        md_obj.create_all(conn)  
        conn.commit()
        msg = f'scheme {stage_scheme} droped. tables created.'
        print(msg)
        s5_common_func.write_journal(msg)
    md_obj.reflect(conn)
    
    for f_path in f_paths:
        # get short filename (without extension)
        short_name = Path(f_path).stem
        
        # get table name by short filename
        table_name = config['stage_layer'][short_name]
        
        # get column names  
        if db_type == 'sqlite':
            col_names = md_obj.tables[table_name].columns.keys()
        elif db_type == 'postgres':
            col_names = md_obj.tables[stage_scheme + '.' + table_name].columns.keys()
        
        # read data from .csv files
        dataset = pd.read_csv(f_path, delimiter = '|', header = None, names = col_names, encoding = 'utf-8')
        
        # insert dataset into database table        
        result = dataset.to_sql(table_name, conn, db_schema, if_exists = 'replace', index = False, chunksize = 1000)
        conn.commit()
        msg = f'{table_name}: inserted'
        print(msg)
        s5_common_func.write_journal(msg)        
      
        # delete .csv files to clean up tmp folder
        os.remove(f_path)
    conn.close()
    
if __name__ == '__main__':
    main()