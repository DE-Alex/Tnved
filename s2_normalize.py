# standart modules
import configparser
import sys, os
import csv
from pathlib import Path
from datetime import date
from datetime import datetime

# developed modules
import s5_common_func

# read configs
config = configparser.ConfigParser() 
config.read(Path(sys.path[0], 'pipeline.conf'))

tmp_folder = Path(sys.path[0], config['general']['tmp_folder'])
code_from = config['data_source']['encode_from']
version_file_name = config['data_source']['version_file_name']

def main():
    # delete possible old .csv files in temp folder
    f_paths = s5_common_func.search_by_mask(tmp_folder, '*.csv')
    for f_path in f_paths:
        os.remove(f_path)    

    # search .txt files in temp folder
    f_paths = s5_common_func.search_by_mask(tmp_folder, '*.txt')
    
    # read and encode DOS (cp866) file
    for txt_path in f_paths:
        with open(txt_path, encoding = code_from) as file:
            data = file.read()
        # split into rows
        data = data.splitlines()
        
        # remove '|' where line ends with
        data = [row[:-1] if row.endswith('|') else row for row in data]
 
        # replace empty columns '\xa0' with ''
        data = [row.replace('\xa0', '') for row in data]
        # split into columns by delimiter '|'
        data = [row.split('|') for row in data]
        
        # take from first line (line [0] in all files) info about TN VED version
        version_info = data[0]
        # convert date to iso-format
        dt_date_from = datetime.strptime(version_info[1], '%Y%m%d')
        version_info[1] = date.isoformat(dt_date_from)
        
        # add filename to version info
        name = Path(txt_path).stem
        file_version = [name] + version_info
        
        # save info about TN VED version in csv file separately 
        filename = version_file_name
        f_ver_path = Path(tmp_folder, version_file_name)
        with open(f_ver_path, 'a', newline='') as csv_file:
            dt_writer = csv.writer(csv_file, delimiter = '|')
            dt_writer.writerow(file_version)
        
        # convert all dates (non empty cells in last 2 columns) to iso-format from 2nd line to last
        # empty sells set to far away date
        dataset = data[1:]
        tmp = []
        for line in dataset:
            dates = line[-2:]
            dates = [date.isoformat(datetime.strptime(dt, '%d.%m.%Y')) if dt != '' else '6666-06-06' for dt in dates]
            line = line[:-2] + dates
            tmp.append(line)
        dataset = tmp
   
        # save lines from 2nd to last to csv file
        name = Path(txt_path).stem
        name = name.lower()
        csv_path = Path(tmp_folder, name+'.csv')
        with open(csv_path, 'w', newline='') as csv_file:
            dt_writer = csv.writer(csv_file, delimiter = '|')
            dt_writer.writerows(dataset)
        
        # delete .txt files to clean up tmp folder
        os.remove(txt_path)
        
    msg = f'decode and normalize - ok'
    print(msg)
    s5_common_func.write_journal(msg)
        
if __name__ == '__main__':
   main()