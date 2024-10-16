import logging
import logging.config
import os, re, json, requests
import pandas as pd
from datetime import datetime
import ipaddress
PARENT_DIR = os.path.abspath(os.path.join(os.path.dirname( __file__ ), os.pardir))
DATETIME = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_ENV = 'dev'
LOG_DIR = 'log'
CONFIG_DIR = 'config'

def get_datetime():
    return datetime.now().strftime("%Y%m%d_%H%M")

def setup_logging() -> None:
    log_configs = {
        "dev": "logging.dev.json", 
        "prod": "logging.prod.json"
        }
    log_config = log_configs.get(LOG_ENV, "logging.dev.json")
    log_config_path = os.path.join(PARENT_DIR, CONFIG_DIR, log_config)
    log_file_path = os.path.join(PARENT_DIR, LOG_DIR, f'pyapicapi_{DATETIME}.log')
    
    with open(log_config_path, 'r') as f:
        config = json.load(f)
    
    # Update the file handler's filename
    for handler in config['handlers'].values():
        if handler['class'] == 'logging.FileHandler':
            handler['filename'] = log_file_path

    logging.config.dictConfig(config)
    return

def get_config_files_to_list(dir: str) -> list:
    matched_files = []
    files = os.listdir(dir)
    for f in files:
        m = re.search("(.*).xlsx",f)
        if m:
            matched_files.append(f)
    return matched_files

def promt_user_select_file(files: list) -> str:
    
    if len(files) == 1:
        logger.info(f' 0) {files[0]}')
        selected_file = files[0]
    else:
        for i in range (len(files)):
            logger.info(f' {str(i)}) {files[i]}')
        print ("---------------------------------------------\n")
        x = int(input("Pick input file from the list above: "))
        print ("\n---------------------------------------------")
        selected_file = files[x]
    logger.info(f' Selected file: {selected_file}')
    return selected_file

def export_df_to_xlsx(writer: pd.ExcelWriter, df: pd.DataFrame, key: str) -> None:
    df.to_excel(writer, sheet_name=key, index=False)
    ws = writer.sheets[key]
    return

def calculate_subnet(ip):
    if pd.isna(ip):
        return ''  # Return empty string for null values
    try:
        # Create an IPv4 network object
        network = ipaddress.ip_network(ip, strict=False)
        return str(network)  # Return the network address as a string
    except ValueError:
        return ''  # Return empty string for invalid gateway formats

def process_script() -> None:
    config_dir = os.path.join(PARENT_DIR)

    # step 1: get config files
    logger.info(f'###### Step1 - Get config files in: {config_dir}')
    files = get_config_files_to_list(config_dir)

    # step 2: user select config file
    logger.info(f'###### Step2 - Choose input config files from list:')
    file = promt_user_select_file(files)

    # step 3: column operation
    # ===========================================================================

    # step 3A: Get system
    # topSystem ========================================
    df_topSystem = pd.read_excel(file, sheet_name='topSystem')
    df_topSystem = df_topSystem[['dn','name','id','fabricId','podId','role','serial','state','version','oobMgmtAddr','inbMgmtAddr','inbMgmtGateway','lastRebootTime','lastResetReason','systemUpTime','tepPool','address']] # choose column
    df_topSystem = df_topSystem.sort_values(by=['dn'])

    # step 3B: Get interface
    # l1PhysIf ========================================
    df_l1PhysIf = pd.read_excel(file, sheet_name='l1PhysIf')
    df_l1PhysIf = df_l1PhysIf[['dn','id','descr','portT','mode','layer','usage','adminSt','autoNeg']]
    df_l1PhysIf = df_l1PhysIf.sort_values(by=['dn'])

    # step 99: export result to xlsx apic_n1_xxxx_20241016_1335.xlsx
    outfile_env = file.split("_")[1]
    outfile = f"apic_{outfile_env}_interface_{get_datetime()}.xlsx"
    writer = pd.ExcelWriter(os.path.join(PARENT_DIR, outfile))
    tshoot = 0
    export_df_to_xlsx(writer, df_topSystem, 'topSystem')
    export_df_to_xlsx(writer, df_l1PhysIf, 'l1PhysIf')

    writer.close()
    return

if __name__ == "__main__":

    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info(f'###')
    logger.info(f'###')
    logger.info(f'############################################################## ')
    logger.info(f'##################       START SCRIPT       ################## ')

    process_script()

    logger.info(f'##################         END SCRIPT       ################## ')
    logger.info(f'############################################################## ')