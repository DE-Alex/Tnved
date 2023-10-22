import sys
import configparser
from pathlib import Path
# read configs
config = configparser.ConfigParser() 
config.read(Path(sys.path[0], 'pipeline.conf'))

scheme = config['stage_layer']['scheme_name']
# table names in stage scheme and core scheme are equal
tb_razdel_name = config['stage_layer']['tb_razdel_name']
tb_gruppa_name = config['stage_layer']['tb_gruppa_name']
tb_tov_poz_name = config['stage_layer']['tb_tov_poz_name']
tb_sub_poz_name = config['stage_layer']['tb_sub_poz_name']
tb_version_name = config['stage_layer']['tb_version_name']

create_scheme =  [
            '''
            CREATE SCHEMA IF NOT EXISTS {0};
            '''.format(scheme)
            ,
            '''
            SET search_path TO {0};
            '''.format(scheme)
            ]
            
create_tables =    [   
            '''
            CREATE TABLE IF NOT EXISTS {0} (
                razdel int2 NOT NULL,
                naim varchar NULL,
                prim varchar NULL,
                date_from varchar NOT NULL,
                expired varchar NULL,
                CONSTRAINT {0}_pkey PRIMARY KEY (razdel, date_from)
            );
            '''.format(tb_razdel_name)
            ,
            '''
            CREATE TABLE IF NOT EXISTS {0} (
                razdel int2 NULL,
                gruppa int2 NOT NULL,
                naim varchar NULL,
                prim varchar NULL,
                date_from varchar NOT NULL,
                expired varchar NULL,
                CONSTRAINT {0}_pkey PRIMARY KEY (gruppa, date_from)
            );            
            '''.format(tb_gruppa_name)
            ,
            '''
            CREATE TABLE IF NOT EXISTS {0} (
                gruppa int2 NOT NULL,
                tov_poz int2 NOT NULL,
                naim varchar NULL,
                date_from varchar NOT NULL,
                expired varchar NULL,
                CONSTRAINT {0}_pkey PRIMARY KEY (gruppa, tov_poz, date_from)
            );             
            '''.format(tb_tov_poz_name)
            ,
            '''
            CREATE TABLE IF NOT EXISTS {0} (
                gruppa int2 NOT NULL,
                tov_poz int2 NOT NULL,
                sub_poz int4 NOT NULL,
                kr_naim varchar NULL,
                date_from varchar NOT NULL,
                expired varchar NULL,
                CONSTRAINT {0}_pkey PRIMARY KEY (gruppa, tov_poz, sub_poz, date_from)
            );
            '''.format(tb_sub_poz_name)
            ,
            '''
            CREATE TABLE IF NOT EXISTS {0} (
                file_name varchar NOT NULL,            
                table_name varchar NOT NULL,
                version varchar NOT NULL,
                date_from varchar NOT NULL,
                some_code varchar NULL,
                CONSTRAINT {0}_pkey PRIMARY KEY (table_name, version, date_from)
            );
            '''.format(tb_version_name)
            ]
    
if __name__ == '__main__': 
	pass