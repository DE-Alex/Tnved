import sys
import configparser
import os, fnmatch
from pathlib import Path
from datetime import datetime
from dateutil.tz import tzlocal
tzlocal = tzlocal()

config = configparser.ConfigParser() 
config.read(Path(sys.path[0], 'pipeline.conf'))

journal_path = Path(sys.path[0], config['general']['logs_folder'], config['general']['journal_file'])

def search_by_mask(folder, mask):
    # search files in folder by mask
    f_paths = []
    for name in os.listdir(folder):
        f = name.lower()
        if fnmatch.fnmatch(f, mask):
            f_path = Path(folder, name)
            f_paths.append(f_path)
    return f_paths
  
def write_journal(msg):
    time_now = datetime.now(tzlocal).replace(microsecond = 0).isoformat()
    with open(journal_path, 'a') as file: 
        file.write(f'{time_now} {msg}\n')
 
if __name__ == '__main__':
    pass
