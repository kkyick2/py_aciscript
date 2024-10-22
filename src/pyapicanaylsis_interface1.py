import logging
import logging.config
import argparse, os, re, json, ipaddress
import pandas as pd
from datetime import datetime
from pathlib import Path
PARENT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
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
    log_file_path = os.path.join(
        PARENT_DIR, LOG_DIR, f'{log_name}_{DATETIME}.log')

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
        m = re.search("(.*).xlsx", f)
        if m:
            matched_files.append(f)
    return matched_files


def promt_user_select_file(files: list) -> str:

    if len(files) == 1:
        logger.info(f' 0) {files[0]}')
        selected_file = files[0]
    else:
        for i in range(len(files)):
            logger.info(f' {str(i)}) {files[i]}')
        print("---------------------------------------------\n")
        x = int(input("Pick input file from the list above: "))
        print("\n---------------------------------------------")
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
    
def start_script(args) -> list:
    logger.info(f'###### Step1')
    infilelist = process_input(args)
    outfilelist = []

    logger.info(f'###### Step2 - Process excel files: {infilelist}')
    i = 0
    for inf in infilelist:
        logger.info(f'###### {i+1}/{len(infilelist)}, process {inf}')
        i = i+1
        outf = process_infile(inf)
        outfilelist.append(outf)

    return outfilelist

def process_input(args) -> list:
    infilelist = []
    # option1: input from cli input 
    if args.infiles:
        logger.info(f'###### Step1 - Get api excel from python arguments:')
        infilelist = args.infiles.split(',')

    # option2: input from promt user select
    if args.infiles == None:
        # step 1: get config files
        logger.info(f'###### Step1 - Get api excel in: {PARENT_DIR}')
        files = get_config_files_to_list(PARENT_DIR)

        # step 2: user select config file
        logger.info(f'###### Step2 - Choose input excel from list:')
        file = promt_user_select_file(files)
        infilelist.append(file)

    return infilelist

def process_infile(file: str) -> None:
    # step 3: column operation
    # ===========================================================================
    logger.info(f'###### Step3 - Process excel: {file}')
    # step 3A: Get system
    # topSystem ========================================
    df_topSystem = pd.read_excel(file, sheet_name='topSystem')
    df_topSystem = df_topSystem[['dn', 'name', 'id', 'fabricId', 'podId', 'role', 'serial', 'state', 'version', 'oobMgmtAddr',
                                 'inbMgmtAddr', 'inbMgmtGateway', 'lastRebootTime', 'lastResetReason', 'systemUpTime', 'tepPool', 'address']]  # choose column
    df_topSystem = df_topSystem.sort_values(by=['dn'])

    # step 3B: Get interface l1PhysIf
    # l1PhysIf ========================================
    df_l1PhysIf = pd.read_excel(file, sheet_name='l1PhysIf')
    df_l1PhysIf = df_l1PhysIf[['dn', 'id', 'descr', 'portT', 'mode', 'layer', 'usage', 'adminSt', 'autoNeg']]  # choose column
    df_l1PhysIf = df_l1PhysIf.sort_values(by=['dn'])

    # step 3B: Get interface ethpmPhysIf
    # ethpmPhysIf ========================================
    df_ethpmPhysIf = pd.read_excel(file, sheet_name='ethpmPhysIf')
    df_ethpmPhysIf = df_ethpmPhysIf[['dn', 'operSpeed', 'operDuplex','operSt', 'operStQual', 'bundleIndex', 'operVlans']]  # choose column
    df_ethpmPhysIf['dn'] = df_ethpmPhysIf['dn'].str.replace(r'/phys$', '', regex=True)
    df_ethpmPhysIf = df_ethpmPhysIf.sort_values(by=['dn'])
    
    # merge
    df_interface =  pd.merge(df_l1PhysIf, df_ethpmPhysIf, on="dn", how="left")
    df_interface = df_interface[['dn', 'id', 'descr', 'portT', 'mode', 'layer', 'usage', 'operSpeed','operDuplex', 'autoNeg', 'adminSt','operSt', 'operStQual', 'bundleIndex', 'operVlans']]

    # step 3C: Get interface epg, encap-vlan
    # fvRsPathAtt ========================================
    df_fvRsPathAtt = pd.read_excel(file, sheet_name='fvRsPathAtt')
    df_fvRsPathAtt = df_fvRsPathAtt[['dn', 'encap', 'tDn']]  # choose column
    df_fvRsPathAtt['dn'] = df_fvRsPathAtt['dn'].str.replace(r'/rspathAtt-\[topology/(.*)\]\]$', '', regex=True)
    df_fvRsPathAtt['encap'] = df_fvRsPathAtt['encap'].str.replace(r'^vlan-', '', regex=True)

    # For output [epg, encap]
    df_epg_encap = df_fvRsPathAtt[['dn', 'encap']]
    df_epg_encap = df_epg_encap.sort_values(by=['encap'])
    df_epg_encap = df_epg_encap.groupby('dn')['encap'].agg(lambda col: ','.join(col.unique())).reset_index()    # group

    # For output [intf, encap]
    df_intf_encap = df_fvRsPathAtt[['tDn', 'encap']]
    df_intf_encap = df_intf_encap.sort_values(by=['encap'])
    df_intf_encap = df_intf_encap.groupby('tDn')['encap'].agg(lambda col: ','.join(col.unique())).reset_index()    # group
    df_intf_encap = df_intf_encap.sort_values(by=['tDn'])

    # step 99: export result to xlsx apic_n1_xxxx_20241016_1335.xlsx
    outfile_env = file.split("_")[1]
    outfile = f"apic_{outfile_env}_interface_{get_datetime()}.xlsx"
    writer = pd.ExcelWriter(os.path.join(PARENT_DIR, outfile))
    tshoot = 1
    export_df_to_xlsx(writer, df_topSystem, 'topSystem')
    export_df_to_xlsx(writer, df_interface, 'interface')
    export_df_to_xlsx(writer, df_epg_encap, 'epg_encap')
    export_df_to_xlsx(writer, df_intf_encap, 'intf_encap')
    if tshoot == 1:
        export_df_to_xlsx(writer, df_l1PhysIf, 'l1PhysIf')
        export_df_to_xlsx(writer, df_ethpmPhysIf, 'ethpmPhysIf')
        export_df_to_xlsx(writer, df_fvRsPathAtt, 'fvRsPathAtt')

    logger.info(f'###')
    logger.info(f'###')
    logger.info(f'### close out file {outfile}')
    logger.info(f'###')
    logger.info(f'###')
    writer.close()
    return


if __name__ == "__main__":

    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info(f'###')
    logger.info(f'###')
    logger.info(f'############################################################## ')
    logger.info(f'##################       START SCRIPT       ################## ')

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--infiles", help="input excel from pyapicapi.py, example: -i n1_20240101.xlsx")
    args = parser.parse_args()

    start_script(args)

    logger.info(f'##################         END SCRIPT       ################## ')
    logger.info(f'############################################################## ')
