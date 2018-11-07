"__author__ = 'Samuel Kozuch'"
"__credits__ = 'Keboola *YYYY*'"
"__project__ = 'processor-split-by-value'"

"""
Python 3 environment 
"""

#import pip
#pip.main(['install', '--disable-pip-version-check', '--no-cache-dir', 'logging_gelf'])

import sys
import os
import logging
import csv
import json
import glob
import pandas as pd
import shutil
import logging_gelf.formatters
import logging_gelf.handlers
from keboola import docker


### Environment setup
abspath = os.path.abspath(__file__)
script_path = os.path.dirname(abspath)
os.chdir(script_path)

### Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)-8s : [line:%(lineno)s] %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")
"""
logger = logging.getLogger()
logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
    host=os.getenv('KBC_LOGGER_ADDR'),
    port=int(os.getenv('KBC_LOGGER_PORT'))
    )
logging_gelf_handler.setFormatter(logging_gelf.formatters.GELFFormatter(null_character=True))
logger.addHandler(logging_gelf_handler)

# removes the initial stdout logging
logger.removeHandler(logger.handlers[0])
"""

logging.debug("Current version is 0.1.1.")

### Access the supplied rules
cfg = docker.Config('/data/')
params = cfg.get_parameters()
nrows = params['nrows']
logging.debug("Data will be split by %s" % str(nrows))

### Get proper list of tables
os.chdir('/data/in/tables/')
in_tables = [i for i in glob.glob('*.{}'.format('csv'))]

os.chdir('/data/out/tables/')
out_tables = [i for i in glob.glob('*.{}'.format('csv'))]

logging.info("IN tables mapped: "+str(in_tables))
logging.info("OUT tables mapped: "+str(out_tables))

### destination to fetch and output files
DEFAULT_FILE_INPUT = "/data/in/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/tables/"

def copy_manifest(in_file, out_file):
    in_path = '/data/in/tables/%s.manifest' % in_file
    out_path = '/data/out/tables/%s.manifest' % out_file

    shutil.copyfile(in_path, out_path)

def main():
    """
    Main execution script.
    """

    in_tables_names = [x[:-4] for x in in_tables]

    for name, table in zip(in_tables_names, in_tables):
        data = pd.read_csv("/data/in/tables/%s" % table, dtype=str, chunksize=nrows)

        chunk_count = 0

        for chunk in data:
            chunk_count += 1
            current_row = chunk_count * nrows
            
            if chunk.shape[0] < nrows:
                current_row = (chunk_count - 1) * nrows + chunk.shape[0]

            filename = name + '_' + str(current_row)
            chunk.to_csv('/data/out/tables/%s.csv' % filename, index=False)

            logging.debug("File %s was written to csv." % filename)
            
        logging.info("File %s was split successfully by increments of %s." % (name, str(nrows)))    

if __name__ == "__main__":

    main()
    logging.debug("The output tables are: %s" % str(os.listdir('/data/out/tables/')))
    logging.info("Done.")