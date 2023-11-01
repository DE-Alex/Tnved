# standart modules
import sys, time
import configparser
import sqlalchemy as sa
import pandas as pd
from pathlib import Path

# developed modules
import s5_common_func
from sql.core_create_objects import create_tables

config = configparser.ConfigParser()
config.read(Path(sys.path[0], 'pipeline.conf'))

#SQLight settings
sqlite_stage_path = Path(sys.path[0], config['sqlite']['sqlite_stage_file'])
sqlite_core_path = Path(sys.path[0], config['sqlite']['sqlite_core_file'])

#table names
tb_razdel_name = config['data_model']['tb_razdel_name']
tb_gruppa_name = config['data_model']['tb_gruppa_name']
tb_tov_poz_name = config['data_model']['tb_tov_poz_name']
tb_sub_poz_name = config['data_model']['tb_sub_poz_name']
tb_version_name = config['data_model']['tb_version_name']

def main():
    start = time.time()
    # connect to "core layer"
    core_engine = sa.create_engine(f'sqlite:///{sqlite_core_path}')
    core_conn = core_engine.connect()
    core_md_obj = sa.MetaData()
    core_md_obj.reflect(core_conn)

    # drop tables if exists
    answ = input(f'{sqlite_core_path}: \nDROP tables if exists? (y)')
    if answ == 'y':
        core_md_obj.drop_all(core_conn)
        core_conn.commit()

    # create tables
    core_md_obj.reflect(core_conn)    
    tables_list = core_md_obj.sorted_tables
    if len(tables_list) == 0:
        core_md_obj = create_tables(core_md_obj)
        core_md_obj.create_all(core_conn)  
        core_conn.commit()
        msg = f'Tables in created.'
        print(msg)
        s5_common_func.write_journal(msg)
    core_md_obj.reflect(core_conn)        

    # connect to "stage layer"
    stage_engine = sa.create_engine(f'sqlite:///{sqlite_stage_path}')
    stage_conn = stage_engine.connect()
    stage_md_obj = sa.MetaData()
    stage_md_obj.reflect(stage_conn)

    # get table names from "stage layer"
    stage_table_names = [table.name for table in stage_md_obj.sorted_tables]

    # read data from database
    print(f'Read from database:')
    db_datasets = {}
    for table_name in stage_table_names:
        print(f'- from {table_name}:', end = '')
        df = pd.read_sql_table(table_name, stage_conn, parse_dates = ['date_from', 'expired'])
        print(f' {len(df)} rows')
        db_datasets[table_name] = df
    
    # join datasets in one structure to arrange relations
    print('join datasets in one structure to arrange relations')
    voc = {}
    # razdel dataset
    df = db_datasets[tb_razdel_name]
    for i in df.index:
        row = list(df.iloc[i])
        r_key = row[0]
        if r_key in voc.keys():
            tmp = voc[r_key]['data']
            tmp.append(row)
            voc[r_key]['data'] = tmp
        else:
            voc[r_key] = {'data' : [row], 'str' : {}}

    # gruppa dataset
    df = db_datasets[tb_gruppa_name]
    for i in df.index:
        row = list(df.iloc[i])
        r_key = row[0]
        gr_key = row[1]
        gr_keys = list(voc[r_key]['str'].keys())
        if gr_key in gr_keys:
            tmp = voc[r_key]['str'][gr_key]['data']
            tmp.append(row)
            voc[r_key]['str'][gr_key]['data'] = tmp
        else:
            voc[r_key]['str'][gr_key] = {'data' : [row], 'str': {}}

    # tov_poz dataset
    df = db_datasets[tb_tov_poz_name]
    for i in df.index:
        row = list(df.iloc[i])
        gr_key = row[0]
        poz_key = row[1]
        for i in voc.keys():
            gr_keys = list(voc[i]['str'].keys())
            if gr_key in gr_keys:
                #append missed data with razdel number
                row = [i] + row
                poz_keys = list(voc[i]['str'][gr_key]['str'].keys())
                if poz_key in poz_keys:
                    tmp = voc[i]['str'][gr_key]['str'][poz_key]['data']
                    tmp.append(row)
                    voc[i]['str'][gr_key]['str'][poz_key]['data'] = tmp
                else:
                    voc[i]['str'][gr_key]['str'][poz_key] = {'data' : [row], 'str':{}}

    # sub_poz dataset
    df = db_datasets[tb_sub_poz_name]
    for i in df.index:
        row = list(df.iloc[i])
        gr_key = row[0]
        poz_key = row[1]
        sub_key = row[2]
        for i in voc.keys():
            gr_keys = list(voc[i]['str'].keys())
            if gr_key in gr_keys:
                #append missed data with razdel number
                row = [i] + row
                poz_keys = list(voc[i]['str'][gr_key]['str'].keys())
                if poz_key in poz_keys:
                    sub_keys = list(voc[i]['str'][gr_key]['str'][poz_key]['str'].keys())
                    if sub_key in sub_keys:
                        tmp = voc[i]['str'][gr_key]['str'][poz_key]['str'][sub_key]['data']
                        tmp.append(row)
                        voc[i]['str'][gr_key]['str'][poz_key]['str'][sub_key]['data'] = tmp
                    else:
                        voc[i]['str'][gr_key]['str'][poz_key]['str'][sub_key] = {'data' : [row]}

    # version dataset
    df = db_datasets[tb_version_name]
    tnved_version_data = [list(df.iloc[i]) for i in df.index]
    print('done')

    print('Split time intervals to exclude overlap:')
    print('- "pozitions" and "subpozitions"')
    #Poz and SubPoz
    for r in voc.keys():
        for g in list(voc[r]['str'].keys()):
            for p in list(voc[r]['str'][g]['str'].keys()):
                p_tmp = voc[r]['str'][g]['str'][p]['data']
                p_dt = correct_dates_intervals(p_tmp)
                voc[r]['str'][g]['str'][p]['data'] = p_dt
                p_dates_joined = [row[-2] for row in p_dt]
                s_dates_joined = []
                for s in list(voc[r]['str'][g]['str'][p]['str'].keys()):
                    s_tmp = voc[r]['str'][g]['str'][p]['str'][s]['data']
                    s_dt = correct_dates_intervals(s_tmp)
                    voc[r]['str'][g]['str'][p]['str'][s]['data'] = s_dt
                    for row in s_dt:
                        s_dates_joined.append(row[-2])
                voc[r]['str'][g]['str'][p]['data'] = correct_start_date(s_dates_joined, p_dt)
                dates = p_dates_joined + s_dates_joined
                periods = gener_periods(dates)
                
                voc[r]['str'][g]['str'][p]['data'] = ext_data(p_dt, periods)

                for s in list(voc[r]['str'][g]['str'][p]['str'].keys()):
                    s_dt = voc[r]['str'][g]['str'][p]['str'][s]['data']
                    voc[r]['str'][g]['str'][p]['str'][s]['data'] = ext_data(s_dt, periods)

    print('- "groups" and "pozitions"')
    #Group and Poz
    for r in voc.keys():
        for g in list(voc[r]['str'].keys()):
            g_tmp = voc[r]['str'][g]['data']
            g_dt = correct_dates_intervals(g_tmp)
            voc[r]['str'][g]['data'] = g_dt
            g_dates_joined = [row[-2] for row in g_dt]
            p_dates_joined = []
            for p in list(voc[r]['str'][g]['str'].keys()):
                p_dt = voc[r]['str'][g]['str'][p]['data']
                for row in p_dt:
                    p_dates_joined.append(row[-2])
            voc[r]['str'][g]['data'] = correct_start_date(p_dates_joined, g_dt)
            dates = g_dates_joined + p_dates_joined
            periods = gener_periods(dates)

            voc[r]['str'][g]['data'] = ext_data(g_dt, periods)

            for p in list(voc[r]['str'][g]['str'].keys()):
                p_dt = voc[r]['str'][g]['str'][p]['data']
                voc[r]['str'][g]['str'][p]['data'] = ext_data(p_dt, periods)

    print('- "razdel" and "groups"')
    #Razdel and Group
    for r in voc.keys():
        r_tmp = voc[r]['data']
        r_dt = correct_dates_intervals(r_tmp)
        voc[r]['data'] = r_dt
        r_dates_joined = [row[-2] for row in r_dt]
        g_dates_joined = []
        for g in list(voc[r]['str'].keys()):
            g_dt = voc[r]['str'][g]['data']
            for row in g_dt:
                g_dates_joined.append(row[-2])
        voc[r]['data'] = correct_start_date(g_dates_joined, r_dt)
        dates = r_dates_joined + g_dates_joined
        periods = gener_periods(dates)
        voc[r]['data'] = ext_data(r_dt, periods)

        for g in list(voc[r]['str'].keys()):
            g_dt = voc[r]['str'][g]['data']
            voc[r]['str'][g]['data'] = ext_data(g_dt, periods)

    print('Reorganize dictionaries into plain datasets')
    razdel_data, gruppa_data, tov_poz_data, sub_poz_data = [], [], [], []
    for i in voc.keys():
        razdel_data = razdel_data + voc[i]['data']
        for j in voc[i]['str'].keys():
            gruppa_data = gruppa_data + voc[i]['str'][j]['data']
            for k in voc[i]['str'][j]['str'].keys():
                tov_poz_data = tov_poz_data + voc[i]['str'][j]['str'][k]['data']
                for l in voc[i]['str'][j]['str'][k]['str'].keys():
                    sub_poz_data = sub_poz_data + voc[i]['str'][j]['str'][k]['str'][l]['data']

    data_sets = [(tb_razdel_name, razdel_data), (tb_gruppa_name, gruppa_data), (tb_tov_poz_name, tov_poz_data), (tb_sub_poz_name, sub_poz_data), (tb_version_name, tnved_version_data)]

    print('Load to database:')
    for table_name, dt_date in data_sets:
        # get column names
        col_names = core_md_obj.tables[table_name].columns.keys()
        try:
            # convert "list" objects to pandas "DataFrames"
            pdt = pd.DataFrame(dt_date, columns = col_names)

            # insert dataset into database table
            result = pdt.to_sql(table_name, core_conn, if_exists = 'append', index = False, method = None, chunksize = 1000)

            core_conn.commit()
            msg = f'- {table_name}: loaded'
            print(msg)
            s5_common_func.write_journal(msg)
        except Exception as e:
            print(e)
    finish = time.time()
    print(f'Finished per {round(finish-start)} sec')

def correct_dates_intervals(dataset):
    # correct end dates of the time intervals to exclude gaps between them
    N = len(dataset)
    # sort rows by "time_from" value
    dataset.sort(key = lambda x: x[-2])
    new_dset = []
    for i in range(N):
        row = dataset[i]
        if i < N-1:
            next_row = dataset[i+1]
            next_date = next_row[-2]
            expired = next_date - pd.Timedelta(days = 1)
        elif i == N-1:
            expired = pd.Timestamp(2030,6,6)
        new_row = row[:-1]
        new_row.append(expired)
        new_dset.append(new_row)
    return new_dset

def correct_start_date(dates, dataset):
    # correct dataset issue: 
    # date (date_from) of the first record in group may be younger
    # than date of the first records in subgroups 
    # that broke foreign keys between them.
    unique_dates = list(set(dates))
    unique_dates.sort()
    min_date = unique_dates[0]
    
    dataset.sort(key = lambda x: x[-2])
    start_row = dataset[0]
    if min_date < start_row[-2]:
        new_row = start_row[:-2] + [min_date] + [start_row[-1]]
        dataset[0] = new_row
    return dataset
    
def gener_periods(dt_dates):
    unique_dates = list(set(dt_dates))
    unique_dates.sort()
    periods = []
    N = len(unique_dates)
    for i in range(N):
        d1 = unique_dates[i]
        if i < N-1:
            d2 = unique_dates[i+1] - pd.Timedelta(days = 1)
        elif i == N-1:
            d2 = pd.Timestamp(2030,6,6)
        periods.append([d1, d2])
    return periods

def ext_data(data, periods):
    extended_data = []
    for row in data:
        date_from = row[-2]
        expired = row[-1]
        if pd.isnull(expired) == True:
            expired = pd.Timestamp(2030,6,6)
        else:
            pass
        for period in periods:
            per_from, per_expired = period
            if date_from <= per_from and per_expired <= expired:
                tmp = row[:-2]
                tmp.extend([per_from, per_expired])
                extended_data.append(tmp)
            else:
                pass
    return extended_data

if __name__ == '__main__':
    main()