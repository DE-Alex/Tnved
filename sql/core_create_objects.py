import sys
import configparser
from pathlib import Path
from sqlalchemy import Table, Column, Integer, String, SmallInteger, Date, PrimaryKeyConstraint, ForeignKeyConstraint

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
                Column('date_from', Date),
                Column('expired',   Date),
                PrimaryKeyConstraint('razdel', 'date_from', name = f'{tb_razdel_name}_pk')
                )
                
    tb_2 = Table(
                tb_gruppa_name,
                metadata_obj,
                Column('razdel',    SmallInteger),                
                Column('gruppa',    SmallInteger),
                Column('naim',      String),
                Column('prim',      String),
                Column('date_from', Date),
                Column('expired',   Date),
                PrimaryKeyConstraint('razdel', 'gruppa', 'date_from', name = f'{tb_gruppa_name}_pk'),
                ForeignKeyConstraint(['razdel', 'date_from'], 
                                    [f'{tb_razdel_name}.razdel', f'{tb_razdel_name}.date_from'])
                )

    tb_3 = Table(
                tb_tov_poz_name,
                metadata_obj,
                Column('razdel',    SmallInteger),                
                Column('gruppa',    SmallInteger),
                Column('tov_poz',   Integer),
                Column('naim',      String),
                Column('date_from', Date),
                Column('expired',   Date),
                PrimaryKeyConstraint('razdel', 'gruppa', 'tov_poz', 'date_from', name = f'{tb_tov_poz_name}_pk'),
                ForeignKeyConstraint(['razdel', 'gruppa', 'date_from'], 
                                    [f'{tb_gruppa_name}.razdel', f'{tb_gruppa_name}.gruppa', f'{tb_gruppa_name}.date_from'])                
                )
            

    tb_4 = Table(
                tb_sub_poz_name,
                metadata_obj,
                Column('razdel',    SmallInteger),                 
                Column('gruppa',    SmallInteger),
                Column('tov_poz',   Integer),
                Column('sub_poz',   Integer),                
                Column('kr_naim',   String),
                Column('date_from', Date),
                Column('expired',   Date),
                PrimaryKeyConstraint('razdel', 'gruppa', 'tov_poz', 'sub_poz', 'date_from', name = f'{tb_sub_poz_name}_pk'),
                ForeignKeyConstraint(['razdel', 'gruppa', 'tov_poz', 'date_from'], 
                                    [f'{tb_tov_poz_name}.razdel', f'{tb_tov_poz_name}.gruppa', f'{tb_tov_poz_name}.tov_poz', f'{tb_tov_poz_name}.date_from'])                         
                )
                
    tb_5 = Table(
                tb_version_name,
                metadata_obj,
                Column('file_name', String),
                Column('table_name',String),
                Column('version',   String),
                Column('date_from', Date),
                Column('some_code', String),
                PrimaryKeyConstraint('table_name', 'version', name = f'{tb_version_name}_pk'),                
                )                
    return metadata_obj

if __name__ == '__main__': 
	pass