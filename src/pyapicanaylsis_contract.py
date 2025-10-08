import logging
import logging.config
import argparse, os, re, json, ipaddress
import pandas as pd
from datetime import datetime
from pathlib import Path
PARENT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__ ), os.pardir))
DATETIME = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_ENV = 'dev'
LOG_DIR = 'log'
CONFIG_DIR = 'config'
CONFIG_DIR_FULL = os.path.join(PARENT_DIR, CONFIG_DIR)

logger = logging.getLogger(__name__)

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

def get_config_files_to_list(dir: str) -> list:
    matched_files = []
    files = os.listdir(dir)
    for f in files:
        m = re.search("(.*).xlsx",f)
        if m:
            matched_files.append(f)
    return matched_files

def prompt_select_file(files: list) -> str:
    
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

def start_script(args) -> list:
    logger.info(f'###### Step1')
    infilelist = process_input(args)
    outfilelist = []

    batch_datetime = getattr(args, 'batch_datetime', get_datetime())

    logger.info(f'###### Step2 - Process excel files: {infilelist}')
    i = 0
    for inf in infilelist:
        logger.info(f'###### {i+1}/{len(infilelist)}, process {inf}')
        i += 1
        outf = process_infile(inf, batch_datetime)
        outfilelist.append(outf)

    return outfilelist

def process_input(args) -> list:
    infilelist = []
    if args.infiles:
        logger.info(f'###### Step1 - Get api excel from python arguments:')
        infilelist = args.infiles.split(',')
        for f in infilelist:
            if not re.match(r'ciscoapic_.*_\d{8}_\d{4}\.xlsx', os.path.basename(f)):
                logger.error(f'Invalid input filename format: {f}')
                infilelist.remove(f)

    if not args.infiles:
        logger.info(f'###### Step1 - Get api excel in: {PARENT_DIR}')
        files = get_config_files_to_list(PARENT_DIR)
        logger.info(f'###### Step2 - Choose input excel from list:')
        file = prompt_select_file(files)
        infilelist.append(file)

    return infilelist

def process_infile(file: str, batch_datetime: str) -> str:
    logger.info(f'###### Step3 - Process excel: {file}')
    
    # Extract environment from filename
    match = re.match(r'ciscoapic_([^_]+)_\d{8}_\d{4}\.xlsx', os.path.basename(file))
    if not match:
        logger.error(f'Invalid input filename format: {file}')
        return ''
    outfile_env = match.group(1)

    # step 3: column operation
    # ===========================================================================
    
    # step 3A: Get subnet
    # fvSubnet ========================================
    df_fvSubnet = pd.read_excel(file, sheet_name='fvSubnet')
    df_fvSubnet = df_fvSubnet[['dn','ip']]                         # choose column
    # dn > epg, ip > gateway
    df_fvSubnet['dn'] = df_fvSubnet['dn'].str.replace(r'/subnet-(.*)$', '', regex=True)

    # step 3B: Get EPG and BD
    # fvRsBd ========================================
    df_fvRsBd = pd.read_excel(file, sheet_name='fvRsBd')
    df_fvRsBd = df_fvRsBd[['dn','tDn']]                             # choose column
    # dn > epg, tdn > bd
    df_fvRsBd['dn'] = df_fvRsBd['dn'].str.replace(r'/rsbd$', '', regex=True)
    # rename column
    df_fvRsBd = df_fvRsBd.rename(columns={'dn':'epg', 'tDn': 'bd'})
    # merge
    df_fvRsBd1 = pd.merge(df_fvRsBd, df_fvSubnet, left_on='epg', right_on='dn', how='inner') # merge epg with ip
    df_fvRsBd1 = df_fvRsBd1.drop(columns=['dn'])
    df_fvRsBd2 = pd.merge(df_fvRsBd, df_fvSubnet, left_on='bd', right_on='dn', how='inner') # merge bd with ip
    df_fvRsBd2 = df_fvRsBd2.drop(columns=['dn'])
    df_fvRsBd3 = pd.concat([df_fvRsBd1, df_fvRsBd2], ignore_index=True, sort=False)  # merge epg and bd with ip
    # Combine output # epg, bd, gateway, subnet
    df_epgbd_ip = pd.merge(df_fvRsBd, df_fvRsBd3, on=['epg', 'bd'], how='left')
    df_epgbd_ip = df_epgbd_ip.sort_values(by=['epg'], ascending=True)
    df_epgbd_ip = df_epgbd_ip.rename(columns={'ip': 'gateway'})
    df_epgbd_ip['subnet'] = df_epgbd_ip['gateway'].apply(calculate_subnet)

    # step 3C: Get contract consumer EPG
    # fvRsCons ========================================
    df_fvRsCons = pd.read_excel(file, sheet_name='fvRsCons')
    df_fvRsCons = df_fvRsCons[['dn','tDn']]                         # choose column
    # dn > epg, tdn > contract
    df_fvRsCons['dn'] = df_fvRsCons['dn'].str.replace(r'/rscons-[^/]+$', '', regex=True)
    # rename column
    df_fvRsCons = df_fvRsCons.rename(columns={'dn':'consumer_epg', 'tDn': 'contract'})     
    
    # step 3D: Get contract provider EPG
    # fvRsProv ========================================
    df_fvRsProv = pd.read_excel(file, sheet_name='fvRsProv')
    df_fvRsProv = df_fvRsProv[['dn','tDn']]                         # choose column
    # dn > epg, tdn > contract
    df_fvRsProv['dn'] = df_fvRsProv['dn'].str.replace(r'/rsprov-[^/]+$', '', regex=True)
    # rename column
    df_fvRsProv = df_fvRsProv.rename(columns={'dn':'provider_epg', 'tDn': 'contract'})     

    # step 3E: Get filters
    # vzRsSubjFiltAtt ========================================
    df_vzRsSubjFiltAtt = pd.read_excel(file, sheet_name='vzRsSubjFiltAtt')
    df_vzRsSubjFiltAtt = df_vzRsSubjFiltAtt[['dn','tnVzFilterName','action']]  # choose column
    # dn > contract, tnVzFilterName > filter
    df_vzRsSubjFiltAtt['dn'] = df_vzRsSubjFiltAtt['dn'].str.replace(r'/subj-(.*)/rssubjFiltAtt-(.*)$', '', regex=True)
    # For output
    df_vzRsSubjFiltAtt_out = df_vzRsSubjFiltAtt.copy()
    df_vzRsSubjFiltAtt_out = df_vzRsSubjFiltAtt_out.rename(columns={'dn':'contract', 'tnVzFilterName': 'filter'})
    df_vzRsSubjFiltAtt_out = df_vzRsSubjFiltAtt_out.sort_values(by=['contract'])
    # For merge
    df_vzRsSubjFiltAtt = df_vzRsSubjFiltAtt[['dn','tnVzFilterName']]                                          # choose column
    df_vzRsSubjFiltAtt = df_vzRsSubjFiltAtt.sort_values(by=['tnVzFilterName'])                                # sorting
    df_vzRsSubjFiltAtt = df_vzRsSubjFiltAtt.groupby('dn')['tnVzFilterName'].agg(lambda col: ','.join(col))    # group the filter by contract name
    df_vzRsSubjFiltAtt = df_vzRsSubjFiltAtt.reset_index(name="tnVzFilterName")                                # add back index
     # rename column
    df_vzRsSubjFiltAtt = df_vzRsSubjFiltAtt.rename(columns={'dn':'contract', 'tnVzFilterName': 'filter'})
    # Combine ========================================
    # consumer_epg, contract, provider_epg, filter
    df_contract_all = pd.merge(df_fvRsCons, df_fvRsProv, on="contract", how="outer")
    df_contract_all = pd.merge(df_contract_all, df_vzRsSubjFiltAtt, on="contract", how="outer")
    # Combine  ========================================
    # contract, consumer_epg, consumer_subnet, provider_epg, provider_subnet, filter 
    df_epgbd_ip_tmp1 = df_epgbd_ip[['epg','subnet']]       # choose column
    df_contract_epgip = pd.merge(df_contract_all, df_epgbd_ip_tmp1, left_on='consumer_epg', right_on='epg', how="left")
    df_contract_epgip = df_contract_epgip.rename(columns={'subnet':'consumer_subnet'})
    df_contract_epgip = pd.merge(df_contract_epgip, df_epgbd_ip_tmp1, left_on='provider_epg', right_on='epg', how="left")
    df_contract_epgip = df_contract_epgip.rename(columns={'subnet':'provider_subnet'})
    df_contract_epgip = df_contract_epgip[['contract','consumer_epg','consumer_subnet','provider_epg','provider_subnet','filter']]

    # step 99: export result to xlsx apic_n1_tables_20241016_1335.xlsx
    outfile_env = file.split("_")[1]
    outfile = f"apic_{outfile_env}_contract_{batch_datetime}.xlsx"
    writer = pd.ExcelWriter(os.path.join(PARENT_DIR, outfile))
    tshoot = 0
    export_df_to_xlsx(writer, df_epgbd_ip, 'epgbd_ip')
    export_df_to_xlsx(writer, df_vzRsSubjFiltAtt_out, 'filter')
    export_df_to_xlsx(writer, df_contract_all, 'contract')
    export_df_to_xlsx(writer, df_contract_epgip, 'contract_epgip')
    if tshoot == 1:
        export_df_to_xlsx(writer, df_fvRsBd, 'fvRsBd')
        export_df_to_xlsx(writer, df_fvSubnet, 'fvSubnet')
        export_df_to_xlsx(writer, df_fvRsCons, 'fvRsCons')
        export_df_to_xlsx(writer, df_fvRsProv, 'fvRsProv')

    logger.info(f'###')
    logger.info(f'###')
    logger.info(f'### close output: {outfile}')
    logger.info(f'###')
    logger.info(f'###')
    writer.close()
    return

def main():
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

if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    main()