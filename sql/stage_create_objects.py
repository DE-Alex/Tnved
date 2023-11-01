import sys
import configparser
from pathlib import Path
from sqlalchemy import Table, Column, Integer, String, SmallInteger, Date, PrimaryKeyConstraint

# read configs
config = configparser.ConfigParser()
config.read(Path(sys.path[0], 'pipeline.conf'))

# table names in stage scheme and core scheme are equal
tb_razdel_name = config['stage_layer']['tb_razdel_name']
tb_gruppa_name = config['stage_layer']['tb_gruppa_name']
tb_tov_poz_name = config['stage_layer']['tb_tov_poz_name']
tb_sub_poz_name = config['stage_layer']['tb_sub_poz_name']
tb_version_name = config['stage_layer']['tb_version_name']

def create_tables(metadata_obj):
    tb_1 = Table(
                tb_razdel_name,
                metadata_obj,
                Column('razdel',    SmallInteger),
                Column('naim',      String),
                Column('prim',      String),
                Column('date_from', String),
                Column('expired',   String),
                PrimaryKeyConstraint('razdel', 'date_from', name = f'{tb_razdel_name}_pk')
                )

    tb_2 = Table(
                tb_gruppa_name,
                metadata_obj,
                Column('razdel',    SmallInteger),                
                Column('gruppa',    SmallInteger),
                Column('naim',      String),
                Column('prim',      String),
                Column('date_from', String),
                Column('expired',   String),
                PrimaryKeyConstraint('razdel', 'gruppa', 'date_from', name = f'{tb_gruppa_name}_pk')
                )

    tb_3 = Table(
                tb_tov_poz_name,
                metadata_obj,
                Column('gruppa',    SmallInteger),
                Column('tov_poz',   Integer),
                Column('naim',      String),
                Column('date_from', String),
                Column('expired',   String),
                PrimaryKeyConstraint('gruppa', 'tov_poz', 'date_from', name = f'{tb_tov_poz_name}_pk')
                )

    tb_4 = Table(
                tb_sub_poz_name,
                metadata_obj,
                Column('gruppa',    SmallInteger),
                Column('tov_poz',   Integer),
                Column('sub_poz',   Integer),              
                Column('kr_naim',   String),
                Column('date_from', String),
                Column('expired',   String),
                PrimaryKeyConstraint('gruppa', 'tov_poz', 'sub_poz', 'date_from', name = f'{tb_sub_poz_name}_pk')                  
                )

    tb_5 = Table(
                tb_version_name,
                metadata_obj,
                Column('file_name', String),
                Column('table_name',String,         primary_key = True),
                Column('version',   String,         primary_key = True),
                Column('date_from', String,           primary_key = True),
                Column('some_code', String),
                PrimaryKeyConstraint('table_name', 'version', 'date_from', name = f'{tb_version_name}_pk')                
                )                
    return metadata_obj

if __name__ == '__main__':
	pass