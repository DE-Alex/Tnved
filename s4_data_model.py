# standart modules
import sys, os
import configparser
from datetime import datetime
from datetime import date, timedelta
from pathlib import Path

# developed modules
import s6_db_operations
import s5_common_func
from sql.core_create_objects import create_scheme
from sql.core_create_objects import create_tables

config = configparser.ConfigParser() 
config.read(Path(sys.path[0], 'pipeline.conf'))

db_type = config['general']['db_type']
# table names in stage scheme and core scheme are equal
tb_razdel_name = config['stage_layer']['tb_razdel_name']
tb_gruppa_name = config['stage_layer']['tb_gruppa_name']
tb_tov_poz_name = config['stage_layer']['tb_tov_poz_name']
tb_sub_poz_name = config['stage_layer']['tb_sub_poz_name']
tb_version_name = config['stage_layer']['tb_version_name']

#Postgres
stage_scheme = config['stage_layer']['scheme_name']
core_scheme = config['core_layer']['scheme_name']

#SQLight
sqlite_stage_path = Path(sys.path[0], config['sqlite']['sqlite_stage_file'])
sqlite_core_path = Path(sys.path[0], config['sqlite']['sqlite_core_file'])

def main():
    # @@@@@remove after testing@@@@@
    if db_type == 'sqlite':
        conn = s6_db_operations.connect_to_db(sqlite_stage_path)
        pass
    elif db_type == 'postgres':
        conn = s6_db_operations.connect_to_db()
    # @@@@@remove after testing@@@@@   

    
    # truncate table
    # tb_periods_name = 'tb_periods'
    # query = 'TRUNCATE {0}.{1} RESTART IDENTITY CASCADE;'.format(core_scheme, tb_periods_name)
    # print(query)
    # curs.execute(query)
    # conn.commit()
    # conn.close

    voc = {}    
    data_table_names = [tb_razdel_name, tb_gruppa_name, tb_tov_poz_name, tb_sub_poz_name]
    
    for name in data_table_names:
        if db_type == 'postgres':
            name = f'{stage_scheme}.{name}'
        query =f'SELECT * FROM {name}'
        curs = conn.cursor()            
        db_data = curs.execute(query).fetchall()
        curs.close
              
        if 'razdel' in name:
            for row in db_data:
                r_key = row[0]
                if r_key in voc.keys():
                    tmp = voc[r_key]['data']
                    tmp.append(row)
                    voc[r_key]['data'] = tmp
                else:
                    voc[r_key] = {'data' : [row], 'str' : {}}
                    
        elif 'gruppa' in name:
            for row in db_data:
                r_key = row[0]
                gr_key = row[1]
                gr_keys = list(voc[r_key]['str'].keys())
                if gr_key in gr_keys:
                    tmp = voc[r_key]['str'][gr_key]['data']
                    tmp.append(row)
                    voc[r_key]['str'][gr_key]['data'] = tmp                    
                else:
                    voc[r_key]['str'][gr_key] = {'data' : [row], 'str': {}}
                    
        elif 'poz' in name and 'sub' not in name:
            for row in db_data:
                gr_key = row[0]
                poz_key = row[1]
                for i in voc.keys():
                    gr_keys = list(voc[i]['str'].keys())
                    if gr_key in gr_keys:
                        # append "razdel" number, missed in source dataset
                        # print(i)
                        # print(row)
                        row = tuple([i] + list(row))                  
                        # print(row)
                        # input()
                    
                        poz_keys = list(voc[i]['str'][gr_key]['str'].keys()) 
                        if poz_key in poz_keys:
                            tmp = voc[i]['str'][gr_key]['str'][poz_key]['data']
                            tmp.append(row)
                            voc[i]['str'][gr_key]['str'][poz_key]['data'] = tmp                       
                        else:
                            voc[i]['str'][gr_key]['str'][poz_key] = {'data' : [row], 'str':{}}
        
        elif 'sub' in name:
            for row in db_data:
                gr_key = row[0]
                poz_key = row[1]
                sub_key = row[2]
                for i in voc.keys():
                    gr_keys = list(voc[i]['str'].keys())
                    if gr_key in gr_keys:
                        poz_keys = list(voc[i]['str'][gr_key]['str'].keys()) 
                        if poz_key in poz_keys:
                            # append "razdel" number, missed in source dataset
                            row = tuple([i] + list(row))
                        
                            sub_keys = list(voc[i]['str'][gr_key]['str'][poz_key]['str'].keys()) 
                            if sub_key in sub_keys:
                                tmp = voc[i]['str'][gr_key]['str'][poz_key]['str'][sub_key]['data']
                                tmp.append(row)
                                voc[i]['str'][gr_key]['str'][poz_key]['str'][sub_key]['data'] = tmp
                            else:
                                voc[i]['str'][gr_key]['str'][poz_key]['str'][sub_key] = {'data' : [row]}
        else:
            pass
    print('sorted')
    import copy
    voc_ext = copy.deepcopy(voc)        
        
    #Poz and SubPoz
    for r in voc_ext.keys():
        for g in list(voc_ext[r]['str'].keys()):
            for p in list(voc_ext[r]['str'][g]['str'].keys()):
                p_dt = voc_ext[r]['str'][g]['str'][p]['data']
                p_dates_joined = [row[-2] for row in p_dt]
                s_dates_joined = []
                for s in list(voc_ext[r]['str'][g]['str'][p]['str'].keys()):
                    s_dt = voc_ext[r]['str'][g]['str'][p]['str'][s]['data']
                    for row in s_dt:
                        s_dates_joined.append(row[-2])
                dates = p_dates_joined + s_dates_joined
                unique_dates = list(set(dates))
                dt_dates = [date.fromisoformat(d) for d in unique_dates]
                dt_dates.sort()
                periods = gener_periods(dt_dates)
                voc_ext[r]['str'][g]['str'][p]['data'] = ext_data(p_dt, periods)
                
                for s in list(voc_ext[r]['str'][g]['str'][p]['str'].keys()):
                    s_dt = voc_ext[r]['str'][g]['str'][p]['str'][s]['data']
                    voc_ext[r]['str'][g]['str'][p]['str'][s]['data'] = ext_data(s_dt, periods)

    #Group and Poz                
    for r in voc_ext.keys():
        for g in list(voc_ext[r]['str'].keys()):
            g_dt = voc_ext[r]['str'][g]['data']
            g_dates_joined = [row[-2] for row in g_dt]
            p_dates_joined = []
            for p in list(voc_ext[r]['str'][g]['str'].keys()):
                p_dt = voc_ext[r]['str'][g]['str'][p]['data']
                for row in p_dt:
                    p_dates_joined.append(row[-2])
            dates = g_dates_joined + p_dates_joined
            unique_dates = list(set(dates))
            dt_dates = [date.fromisoformat(d) for d in unique_dates]
            dt_dates.sort()
            periods = gener_periods(dt_dates)
            
            voc_ext[r]['str'][g]['data'] = ext_data(g_dt, periods)
            
            for p in list(voc_ext[r]['str'][g]['str'].keys()):
                p_dt = voc_ext[r]['str'][g]['str'][p]['data']
                voc_ext[r]['str'][g]['str'][p]['data'] = ext_data(p_dt, periods)

    #Razdel and Group
    for r in voc_ext.keys():
        #for g in list(voc_ext[r]['gruppa'].keys()):
        r_dt = voc_ext[r]['data']
        r_dates_joined = [row[-2] for row in r_dt]
        g_dates_joined = []
        for g in list(voc_ext[r]['str'].keys()):
            g_dt = voc_ext[r]['str'][g]['data']
            for row in g_dt:
                g_dates_joined.append(row[-2])
        dates = r_dates_joined + g_dates_joined
        unique_dates = list(set(dates))
        dt_dates = [date.fromisoformat(d) for d in unique_dates]
        dt_dates.sort()
        periods = gener_periods(dt_dates)
        
        voc_ext[r]['data'] = ext_data(r_dt, periods)
        
        for g in list(voc_ext[r]['str'].keys()):
            g_dt = voc_ext[r]['str'][g]['data']
            voc_ext[r]['str'][g]['data'] = ext_data(g_dt, periods)
    
    razdel_data = []
    gruppa_data = []
    tov_poz_data = []
    sub_poz_data = []
    
    for i in voc_ext.keys():
        razdel_data = razdel_data + voc_ext[i]['data']
        for j in voc_ext[i]['str'].keys():
            gruppa_data = gruppa_data + voc_ext[i]['str'][j]['data']
            for k in voc_ext[i]['str'][j]['str'].keys():
                tov_poz_data = tov_poz_data + voc_ext[i]['str'][j]['str'][k]['data']
                for l in voc_ext[i]['str'][j]['str'][k]['str'].keys():
                    sub_poz_data = sub_poz_data + voc_ext[i]['str'][j]['str'][k]['str'][l]['data']

    tmp = [(tb_razdel_name, razdel_data), (tb_gruppa_name, gruppa_data), (tb_tov_poz_name, tov_poz_data), (tb_sub_poz_name, sub_poz_data)] 
                    
    # @@@@@remove after testing@@@@@
    a = input(f'Drop scheme "{core_scheme}"? (y)')
    if a == 'y' and db_type == 'sqlite':
        # delete .sqlite3 file to create new scheme
        os.remove(sqlite_core_path)
        conn = s6_db_operations.connect_to_db(sqlite_core_path)
    elif a != 'y' and db_type == 'sqlite':
        conn = s6_db_operations.connect_to_db(sqlite_core_path)
    elif a == 'y' and db_type == 'postgres':
        conn = s6_db_operations.connect_to_db()
        s6_db_operations.drop_scheme(conn, core_scheme)
        msg = f'scheme {core_scheme} droped'
        print(msg)
        s5_common_func.write_journal(msg)
        # create scheme 
        curs = conn.cursor()
        for query in create_scheme:
            curs.execute(query)
        curs.close()
        msg = f'scheme {core_scheme} created'
        print(msg)
        s5_common_func.write_journal(msg)
    elif a != 'y' and db_type == 'postgres':
        conn = s6_db_operations.connect_to_db()
            
    
    # check db tables and create if not exist 
    curs = conn.cursor()    
    for query in create_tables:
        curs.execute(query)
    curs.close()
    conn.commit()
    msg = f'"CREATE tables IF NOT EXIST" - ok'
    print(msg)
    s5_common_func.write_journal(msg)
  
 
    # @@@@@remove after testing@@@@@   

    
    # truncate table
    # tb_periods_name = 'tb_periods'
    # query = 'TRUNCATE {0}.{1} RESTART IDENTITY CASCADE;'.format(core_scheme, tb_periods_name)
    # print(query)
    # curs.execute(query)
    # conn.commit()
    # conn.close                    
          
    for table_name, dt in tmp:
        if db_type == 'postgres':
            name = f'{core_scheme}.{name}'
        print(table_name)
        # get column names from table
        #table_name = f'{core_scheme}.{name}'
        table_columns = s6_db_operations.select_col_names(conn, table_name)
        #columns = table_columns + all_period_columns
        #input('?')
        #insert data
        s6_db_operations.insert(conn, table_name, table_columns, dt)
        print(f'{table_name} - ok')

def gener_periods(dt_dates):    
    periods = []
    N = len(dt_dates)
    for i in range(N):
        d1 = dt_dates[i]
        if i < N-1:
            d2 = dt_dates[i+1] - timedelta(days = 1)
        elif i == N-1:
            d2 = date(6666,6,6) #dt_dates[i] + - timedelta(years = 1000)
        periods.append([str(d1), str(d2)])
    
    for period in periods:  
        if periods.count(period) > 1:
            print(periods)
            input()
    return periods

def ext_data(data, periods):
    extended_data = []
    for row in data:
        date_from = date.fromisoformat(row[-2])
        expired = row[-1]
        if expired == None or expired == '':
            expired = date(6666,6,6)
        else:
            expired = date.fromisoformat(expired)
        
        for period in periods:
            time_from, expired2 = period
            time_from = date.fromisoformat(time_from)
            expired2 = date.fromisoformat(expired2)

            if date_from <= time_from and expired2 <= expired:
                tmp = list(row)
                tmp[-1] = str(expired2)
                tmp[-2] = str(time_from)
                
                extended_data.append(tmp)
            else:
                pass
    return extended_data

# def calc(voc):
    # razd = 0
    # grupp = 0
    # poz = 0
    # sub = 0
    # for i in voc.keys():
        # if i != 'str':
            # razd = razd + len(voc[i]['data'])
            # for j in list(voc[i]['str'].keys()):
                # if j != 'str':
                    # grupp = grupp + len(voc[i]['str'][j]['data'])
                    # for k in list(voc[i]['str'][j]['str'].keys()):
                        # if k != 'str':
                            # poz = poz + len(voc[i]['str'][j]['str'][k]['data'])
                            # for l in list(voc[i]['str'][j]['str'][k]['str'].keys()):
                                # if l != 'str':
                                    # sub = sub + len(voc[i]['str'][j]['str'][k]['str'][l]['data'])
    # print(f'{razd}  {grupp} {poz}  {sub}')        
        
        

    
'''   
def transform(inp, start):
    for i in inp.keys():

        if start != 3:
            start = start + 1
            result = transform(inp[i]['str'], start)
            inp = result
        
        h_dt = inp[i]['data']
        h_dates_joined = [row[-2] for row in h_dt]
        l_dates_joined = []
        for l in list(inp[i]['str'].keys()):
            l_dt = inp['str'][i]['str'][l]['data']
            for row in l_dt:
                l_dates_joined.append(row[-2])
        dates = h_dates_joined + l_dates_joined
        unique_dates = list(set(dates))
        dt_dates = [date.fromisoformat(d) for d in unique_dates]
        dt_dates.sort()
        periods = gener_periods(dt_dates)
        
        inp[i]['data'] = ext_data(p_dt, periods)
        
        for l in list(inp[i]['str'].keys()):
            s_dt = inp[i]['str'][l]['data']
            inp[i]['str'][l]['data'] = ext_data(s_dt, periods)
    return result
'''
    

 
if __name__ == '__main__':
    main()