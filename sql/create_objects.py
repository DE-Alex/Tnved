import sys
import configparser
from pathlib import Path
# read configs
config = configparser.ConfigParser() 
config.read(Path(sys.path[0], 'pipeline.conf'))

scheme = config['db_scheme']['scheme_name']

tb_razdel_name = config['db_scheme']['tb_razdel_name']
tb_gruppa_name = config['db_scheme']['tb_gruppa_name']
tb_tov_poz_name = config['db_scheme']['tb_tov_poz_name']
tb_sub_poz_name = config['db_scheme']['tb_sub_poz_name']
tb_version_name = config['db_scheme']['tb_version_name']

actions = [  
            '''
            CREATE SCHEMA IF NOT EXISTS {0};
            '''.format(scheme)
            ,
            '''
            CREATE TABLE IF NOT EXISTS {0}.{1} (
                razdel int2 NOT NULL,
                naim varchar NULL,
                prim varchar NULL,
                date_from varchar NOT NULL,
                expired varchar NULL,
                CONSTRAINT {1}_pkey PRIMARY KEY (razdel, date_from)
            );
            '''.format(scheme, tb_razdel_name)
            ,
            '''
            CREATE TABLE IF NOT EXISTS {0}.{1} (
                razdel int2 NULL,
                gruppa int2 NOT NULL,
                naim varchar NULL,
                prim varchar NULL,
                date_from varchar NOT NULL,
                expired varchar NULL,
                CONSTRAINT {1}_pkey PRIMARY KEY (gruppa, date_from)
            );            
            '''.format(scheme, tb_gruppa_name)
            ,
            '''
            CREATE TABLE IF NOT EXISTS {0}.{1} (
                gruppa int2 NOT NULL,
                tov_poz int2 NOT NULL,
                naim varchar NULL,
                date_from varchar NOT NULL,
                expired varchar NULL,
                CONSTRAINT {1}_pkey PRIMARY KEY (gruppa, tov_poz, date_from)
            );             
            '''.format(scheme, tb_tov_poz_name)
            ,
            '''
            CREATE TABLE IF NOT EXISTS {0}.{1} (
                gruppa int2 NOT NULL,
                tov_poz int2 NOT NULL,
                sub_poz int4 NOT NULL,
                kr_naim varchar NULL,
                date_from varchar NOT NULL,
                expired varchar NULL,
                CONSTRAINT {1}_pkey PRIMARY KEY (gruppa, tov_poz, sub_poz, date_from)
            );
            '''.format(scheme, tb_sub_poz_name)
            ,
            '''
            CREATE TABLE IF NOT EXISTS {0}.{1} (
                table_name varchar NOT NULL,
                version varchar NOT NULL,
                date_from varchar NOT NULL,
                some_code varchar NULL,
                CONSTRAINT {1}_pkey PRIMARY KEY (table_name, version, date_from)
            );
            '''.format(scheme, tb_version_name)
         ]
         
if __name__ == '__main__': 
	pass