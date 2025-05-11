import logging
import os
import shutil

from finance.constants import TIMESTAMP

LOG_DIR = "logs"

def get_log_file_name():
    return f"log_{TIMESTAMP}.log"

LOG_FILE_NAME = get_log_file_name()

# Clearing old log files
if os.path.exists(LOG_DIR):
    shutil.rmtree(LOG_DIR)

os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE_PATH = os.path.join(LOG_DIR, LOG_FILE_NAME)

logging.basicConfig(filename= LOG_FILE_PATH,
                    filemode= "w",
                    format = '[%(asctime)s] \t%(levelname)s \t%(filename)s \t%(lineno)d \t%(funcName)s\t%(message)s',
                    
                    level= logging.INFO
                    )

logger = logging.getLogger("FinanceProductComplaint")