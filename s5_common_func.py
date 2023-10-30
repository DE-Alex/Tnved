import sys
import configparser
import glob
from pathlib import Path
from datetime import datetime
from dateutil.tz import tzlocal
tzlocal = tzlocal()

config = configparser.ConfigParser() 
config.read(Path(sys.path[0], 'pipeline.conf'))

journal_path = Path(sys.path[0], config['general']['logs_folder'], config['general']['journal_file'])

def search_by_mask(folder, mask):
    # search files in folder by mask
    mask_path = str(Path(folder, mask))
    f_paths = glob.glob(mask_path)
    return f_paths
  
def write_journal(msg):
    time_now = datetime.now(tzlocal).replace(microsecond = 0).isoformat()
    with open(journal_path, 'a') as file: 
        file.write(f'{time_now} {msg}\n')
 
if __name__ == '__main__':
    pass