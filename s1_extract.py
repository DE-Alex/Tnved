# standart modules
import configparser
import sys, os
import time
import requests
import zipfile
import io
from pathlib import Path

# developed modules
import s5_common_func

# read configs
config = configparser.ConfigParser() 
config.read(Path(sys.path[0], 'pipeline.conf'))

tmp_folder = Path(sys.path[0], config['general']['tmp_folder'])
url = config['data_source']['url']

def main():
    # delete possible old .txt files in temp folder
    f_paths = s5_common_func.search_by_mask(tmp_folder, '*.txt')
    for f_path in f_paths:
        os.remove(f_path)
    
    # download zip file
    payload = download_data(url)
    # path = Path(tmp_folder, 'TNVED.ZIP')
    # with open(path, 'rb') as file:
        # payload = file.read()
    
    # extract txt files
    z_file = zipfile.ZipFile(io.BytesIO(payload))
    z_file.extractall(path = tmp_folder)
    
    msg = f'unzip arhive - ok'
    print(msg)
    s5_common_func.write_journal(msg)
            
def download_data(url):
    i = 0

    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
    my_headers={'User-Agent' : user_agent,
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language' : 'en-US,en;q=0.5',
            'Accept-Encoding' : 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'TE': 'trailers'}

    while True:
        req = requests.get(url, headers = my_headers)
        req.close()
        if req.status_code == requests.codes.ok:
            payload = req.content
            msg = f'download - ok'
            print(msg)
            s5_common_func.write_journal(msg)
            return payload
        else:
            msg = f'download error: status code {req.status_code}. Paused for 3 sec.'
            print(msg)
            s5_common_func.write_journal(msg)
            i = i + 1    
            time.sleep(3)
        if i >= 5:
            msg = f'download failed. Exit.'
            s5_common_func.write_journal(msg)
            exit(1)          
          
if __name__ == '__main__':
   main()