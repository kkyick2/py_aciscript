from pyapicanaylsis_interface import main
import logging
import logging.config
import argparse, os, re, json, requests
import pandas as pd
from datetime import datetime
from pathlib import Path
import pyapicanaylsis_interface
import pyapicanaylsis_contract
PARENT_DIR = os.path.abspath(os.path.join(os.path.dirname( __file__ ), os.pardir))
DATETIME = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_ENV = 'dev'
LOG_DIR = 'log'
CONFIG_DIR = 'config'
CONFIG_DIR_FULL = os.path.join(PARENT_DIR, CONFIG_DIR)

def get_datetime():
    return datetime.now().strftime("%Y%m%d_%H%M")

def setup_logging() -> None:
    log_configs = {
        "dev": "logging.dev.json", 
        "prod": "logging.prod.json"
        }
    log_name = Path(os.path.basename(__file__)).stem # filename without extension
    log_config = log_configs.get(LOG_ENV, "logging.dev.json")
    log_config_path = os.path.join(PARENT_DIR, CONFIG_DIR, log_config)
    log_file_path = os.path.join(PARENT_DIR, LOG_DIR, f'{log_name}_{DATETIME}.log')
    
    with open(log_config_path, 'r') as f:
        config = json.load(f)
    
    # Update the file handler's filename
    for handler in config['handlers'].values():
        if handler['class'] == 'logging.FileHandler':
            handler['filename'] = log_file_path

    logging.config.dictConfig(config)
    return

def this_main():
    outfilelist = ['apic_n1_20241023_1031.xlsx']
    for file in outfilelist:
        args2 = argparse.Namespace()
        args2.infiles = file
        print(args2)

    pyapicanaylsis_interface.main()
    pyapicanaylsis_contract.main()

if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    this_main()