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

def replace_value(df, column, regex_list):       #replace pattern
    df[column] = df[column].replace(regex=regex_list, value="")
    return df

def remove_columns(df: pd.DataFrame, properties: list) -> pd.DataFrame:
    for i in properties:
        df.pop(i)
    logger.info(f' New size: {df.shape}')
    return df

def select_columns(df: pd.DataFrame, properties: list) -> pd.DataFrame:
    df = df[properties]
    logger.info(f' New size: {df.shape}')
    return df

def calculate_subnet(ip):
    return str(ipaddress.ip_network(ip, strict=False))

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

    # Get all subnet for temp use

    # Get all EPG for temp use    

    # Merge ip to corresponding EGP & BD

    # Get contract consumer EPG 
    # fvRsCons ========================================
    df_fvRsCons = pd.read_excel(file, sheet_name='fvRsCons')
    df_fvRsCons = df_fvRsCons[['dn','tDn']]                         # choose column
    # dn > epg, tdn > contract
    df_fvRsCons['dn'] = df_fvRsCons['dn'].str.replace(r'/rscons-[^/]+$', '', regex=True)
    # rename column
    df_fvRsCons = df_fvRsCons.rename(columns={'dn':'consumer_epg', 'tDn': 'contract'})     
    
    # Get contract provider EPG 
    # fvRsProv ========================================
    df_fvRsProv = pd.read_excel(file, sheet_name='fvRsProv')
    df_fvRsProv = df_fvRsProv[['dn','tDn']]                         # choose column
    # dn > epg, tdn > contract
    df_fvRsProv['dn'] = df_fvRsProv['dn'].str.replace(r'/rsprov-[^/]+$', '', regex=True)
    # rename column
    df_fvRsProv = df_fvRsProv.rename(columns={'dn':'provider_epg', 'tDn': 'contract'})     

    # Get filters in contract
    # vzRsSubjFiltAtt ========================================
    df_vzRsSubjFiltAtt = pd.read_excel(file, sheet_name='vzRsSubjFiltAtt')
    df_vzRsSubjFiltAtt = df_vzRsSubjFiltAtt[['dn','tnVzFilterName']] # choose column
    # dn > contract, tnVzFilterName > filter
    df_vzRsSubjFiltAtt['dn'] = df_vzRsSubjFiltAtt['dn'].str.replace(r'/subj-(.*)/rssubjFiltAtt-(.*)$', '', regex=True)
    df_vzRsSubjFiltAtt = df_vzRsSubjFiltAtt.sort_values(by=['tnVzFilterName'])                                # sorting
    df_vzRsSubjFiltAtt = df_vzRsSubjFiltAtt.groupby('dn')['tnVzFilterName'].agg(lambda col: ','.join(col))    # group the filter by contract name
    df_vzRsSubjFiltAtt = df_vzRsSubjFiltAtt.reset_index(name="tnVzFilterName")                                # add back index
    # rename column
    df_vzRsSubjFiltAtt = df_vzRsSubjFiltAtt.rename(columns={'dn':'contract', 'tnVzFilterName': 'filter'})
 
    # Combine consumer_epg, provider_epg, filter
    df_contractcombine = pd.merge(df_fvRsCons, df_fvRsProv, on="contract", how="outer")
    df_contractcombine = pd.merge(df_contractcombine, df_vzRsSubjFiltAtt, on="contract", how="outer")

    # step 99: export result to xlsx
    outfile = f"apic_tables_{get_datetime()}.xlsx"
    writer = pd.ExcelWriter(os.path.join(PARENT_DIR, outfile))
    export_df_to_xlsx(writer, df_fvRsCons, 'fvRsCons')
    export_df_to_xlsx(writer, df_fvRsProv, 'fvRsProv')
    export_df_to_xlsx(writer, df_vzRsSubjFiltAtt, 'vzRsSubjFiltAtt')
    export_df_to_xlsx(writer, df_contractcombine, 'contract_combine')

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