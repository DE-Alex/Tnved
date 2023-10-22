import sys
import configparser
from pathlib import Path
# read configs
config = configparser.ConfigParser() 
config.read(Path(sys.path[0], 'pipeline.conf'))

core_scheme = config['core_layer']['scheme_name']
# table names in stage scheme and core scheme are equal
tb_razdel_name = config['stage_layer']['tb_razdel_name']
tb_gruppa_name = config['stage_layer']['tb_gruppa_name']
tb_tov_poz_name = config['stage_layer']['tb_tov_poz_name']
tb_sub_poz_name = config['stage_layer']['tb_sub_poz_name']
tb_version_name = config['stage_layer']['tb_version_name']

create_scheme =  [
                '''
                CREATE SCHEMA IF NOT EXISTS {0};
                '''.format(core_scheme)
                ,
                '''
                SET search_path TO {0};
                '''.format(core_scheme)
                ]

create_tables = [  

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
                    razdel int2 NOT NULL,
                    gruppa int2 NOT NULL,
                    naim varchar NULL,
                    prim varchar NULL,
                    date_from varchar NOT NULL,
                    expired varchar NULL,
                    FOREIGN KEY (razdel, date_from)  REFERENCES {1} (razdel, date_from),
                    CONSTRAINT {0}_pkey PRIMARY KEY (razdel, gruppa, date_from)
                );            
                '''.format(tb_gruppa_name,tb_razdel_name )
                ,
                '''
                CREATE TABLE IF NOT EXISTS {0} (
                    razdel int2 NOT NULL,                
                    gruppa int2 NOT NULL,
                    tov_poz int2 NOT NULL,
                    naim varchar NULL,
                    date_from varchar NOT NULL,
                    expired varchar NULL,
                    FOREIGN KEY (razdel, gruppa, date_from)  REFERENCES {1} (razdel, gruppa, date_from),
                    CONSTRAINT {0}_pkey PRIMARY KEY (razdel, gruppa, tov_poz, date_from)
                );             
                '''.format(tb_tov_poz_name, tb_gruppa_name)
                ,
                '''
                CREATE TABLE IF NOT EXISTS {0} (
                    razdel int2 NOT NULL,                
                    gruppa int2 NOT NULL,
                    tov_poz int2 NOT NULL,
                    sub_poz int4 NOT NULL,
                    kr_naim varchar NULL,
                    date_from varchar NOT NULL,
                    expired varchar NOT NULL,
                    FOREIGN KEY (razdel, gruppa, tov_poz, date_from)  REFERENCES {1} (razdel, gruppa, tov_poz, date_from),
                    CONSTRAINT {0}_pkey PRIMARY KEY (razdel, gruppa, tov_poz, sub_poz, date_from, expired)
                );
                '''.format(tb_sub_poz_name, tb_tov_poz_name)
                ,
                                '''
                CREATE TABLE IF NOT EXISTS {0} (
                    file_name varchar NOT NULL,
                    table_name varchar NOT NULL,
                    version varchar NOT NULL,
                    date_from varchar NOT NULL,
                    some_code varchar NULL,
                    CONSTRAINT {0}_pkey PRIMARY KEY (table_name, version)
                );
                '''.format(tb_version_name)
                ]
         
if __name__ == '__main__': 
	pass