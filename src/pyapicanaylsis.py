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
    # fvAEPg

    # fvBD
    
    # fvRsBd ==================== (BD EPG mapping)
    df_fvRsBd = pd.read_excel(file, sheet_name='fvRsBd')
    # 'in': dn,tnFvBDName
    # 'out': ap,epg,bd
    #
    # dn:[0]   [1]    [2]        [3]            [4] 
    # uni/tn-tn-eMPF/ap-ap-MDG/epg-epg-MDG-dAPP/rsbd
    df_fvRsBd_split = df_fvRsBd['dn'].str.split('/', expand=True)
    df_fvRsBd_out = df_fvRsBd_split[[2, 3]].rename(columns={2: 'ap', 3: 'epg'})
    df_fvRsBd_out['ap'] = df_fvRsBd_out['ap'].str.replace(r'ap-ap-(.*)', r'ap-\1', regex=True)
    df_fvRsBd_out['epg'] = df_fvRsBd_out['epg'].str.replace(r'epg-epg-(.*)', r'epg-\1', regex=True)
    df_fvRsBd_out['bd'] = df_fvRsBd['tnFvBDName']

    # fvSubnet ==================== (Subnet map with EPG, BD)
    df_fvSubnet = pd.read_excel(file, sheet_name='fvSubnet')
    # in: dn,ip
    # out: dn,gateway,subnet
    #  
    # dn
    # uni/tn-tn-eMPF/ap-ap-PBI/epg-epg-PBI-preDB/subnet-[1.2.2.30/28] <-case1
    # uni/tn-tn-eMPF/BD-bd-Splunk-l2s/subnet-[10.231.3.254/24]  <-case2
    # uni/tn-mgmt/BD-inb/subnet-[10.211.255.254/23] <-case3
    df_fvSubnet['dn'] = df_fvSubnet['dn'].str.extract(r'(epg-epg-[^/]+|BD-[^/]+)/')[0]
    df_fvSubnet['dn'] = df_fvSubnet['dn'].str.replace(r'epg-epg-(.*)', r'epg-\1', regex=True)
    df_fvSubnet['dn'] = df_fvSubnet['dn'].str.replace(r'BD-(.*)', r'\1', regex=True)
    df_fvSubnet_out = df_fvSubnet[['dn', 'ip']]
    # calc ip address to subnet address
    df_fvSubnet_out = df_fvSubnet_out.rename(columns={'ip': 'gateway'})
    df_fvSubnet_out['subnet'] = df_fvSubnet_out['gateway'].apply(calculate_subnet)
    
    # merage
    # temp_df1 = pd.merge(df_fvRsBd_out, df_fvSubnet_out, on="epg", how="outer")    

    # step 99: export result to xlsx
    outfile = f"apic_tables_{get_datetime()}.xlsx"
    writer = pd.ExcelWriter(os.path.join(PARENT_DIR, outfile))
    export_df_to_xlsx(writer, df_fvRsBd_out, 'fvRsBd')
    export_df_to_xlsx(writer, df_fvSubnet_out, 'fvSubnet')
    # export_df_to_xlsx(writer, temp_df1, 'temp_df1')
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