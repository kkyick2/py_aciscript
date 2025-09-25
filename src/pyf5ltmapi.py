import logging
import logging.config
import argparse, os, re, json, requests
import pandas as pd
from datetime import datetime
from pathlib import Path
import pyapicanaylsis_interface
import pyapicanaylsis_contract
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
    log_file_path = os.path.join(PARENT_DIR, LOG_DIR, f'{log_name}_{DATETIME}.log')
    
    with open(log_config_path, 'r') as f:
        config = json.load(f)
    
    # Update the file handler's filename
    for handler in config['handlers'].values():
        if handler['class'] == 'logging.FileHandler':
            handler['filename'] = log_file_path

    logging.config.dictConfig(config)
    return

def get_f5ltm_token(ip: str, username: str, password: str) -> str:
    url = f'https://{ip}/mgmt/shared/authn/login'
    payload = {
        "username": username,
        "password": password,
        "loginProviderName": "tmos"
    }
    headers = {
        "Accept":"application/json",
        "Content-Type": "application/json"
    }
    requests.packages.urllib3.disable_warnings()
    resp = requests.post(url, headers=headers, data=json.dumps(payload), verify=False)
    token = resp.json()['token']['token']

    logger.info(f'Token: {token}')
    return token

def get_f5ltm_api_resp(ip: str, key: str, token: str) -> requests.Response:
    url = f'https://{ip}/mgmt/tm/ltm/{key}'
    payload={}
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-F5-Auth-Token" : f'{token}'
    }

    logger.info(f' Login api: {key} - {url}')
    resp = requests.get(url, headers=headers, data=payload, verify=False)
    return resp

def parse_f5ltm_json(json_obj: list, key: str) -> pd.DataFrame:
    # parse json to dataframe
    # json_obj = { "kind": "xx", "selfLink": "xx", "items": [{"kind": "xx","name": "xx", "partition": "xx", ...
    parsed_data = []
    for data in json_obj['items']:
        parsed_data.append(data)
    df = pd.DataFrame(parsed_data)
    logger.info(f' Exported to dataframe, size: {df.shape}')
    return df

def remove_columns(df: pd.DataFrame, properties: list) -> pd.DataFrame:
    for i in properties:
        if i in df.columns:  # Check if column exists before removing
            df.pop(i)
    logger.info(f' Removed properties size: {df.shape}')
    return df

def export_df_to_xlsx(writer: pd.ExcelWriter, df: pd.DataFrame, key: str) -> None:
    df.to_excel(writer, sheet_name=key, index=False)
    ws = writer.sheets[key]
    return

def get_config_files_to_list(dir: str) -> list:
    matched_files = []
    files = os.listdir(dir)
    for f in files:
        m = re.search("(.*)ltm(.*).json", f)
        if m:
            matched_files.append(f)
    return matched_files

def prompt_select_file(files: list) -> str:
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

def read_config_json(in_file: str) -> list:
    '''
        config json file format (all_apic_example.json):
        {
            "devices": [
                {
                    "environment": "n1",
                    "ip": "172.31.1.1:443",
                    "username": "admin",
                    "password": "pass",
                    "tables": "apic_tables.json"
                },
                ...
            ]
        }
        tables json file format (apic_tables.json):
        {
            "remove_properties_flag": 1,
            "tables": [
                {
                    "name": "Loading APIC Topology",
                    "key": "topSystem",
                    "alias": "Topology",
                    "remove_properties": [
                        "oobMgmtGateway",
                        "configIssues"
                    ]
                }
            ]
        }
    '''
    
    with open(in_file, 'r') as f:
        config = json.load(f)
    
    # Load the tables from the referenced tables file
    tables_file = os.path.join(CONFIG_DIR_FULL, config['devices'][0]['tables'])
    with open(tables_file, 'r') as f:
        tables_config = json.load(f)
    
    logger.info(f" Info collected, tables to process: {len(tables_config['tables'])} for {len(config['devices'])} devices")
    return [config['devices'], tables_config['tables'], tables_config['remove_properties_flag']]

def start_script(args) -> list:
    logger.info(f'###')
    logger.info(f'###')
    logger.info(f'############################################################## ')
    logger.info(f'##################       START SCRIPT       ################## ')
    logger.info(f'###### Step1')
    infilelist = process_input(args)
    outfilelist = []

    logger.info(f'###### Step2 - Process config files: {infilelist}')
    i = 0
    for inf in infilelist:
        logger.info(f'###### {i+1}/{len(infilelist)}, process {inf}')
        i += 1
        outf = process_infile(inf)
        outfilelist.extend(outf)  # Extend to handle multiple outputs from one file
    logger.info(f'###### Complete, outfiles: {outfilelist}')
    return outfilelist

def process_input(args) -> list:
    infilelist = []
    # Option 1: input from CLI input 
    if args.infiles:
        logger.info(f'###### Step1 - Get config files from python arguments:')
        infilelist = args.infiles.split(',')

    # Option 2: input from prompt user select
    if not args.infiles:
        # Step 1: get config files
        logger.info(f'###### Step1 - Get config files in folder: {CONFIG_DIR_FULL}')
        files = get_config_files_to_list(CONFIG_DIR_FULL)

        # Step 2: user select config file
        logger.info(f'###### Step1 - Choose input config files from list:')
        file = prompt_select_file(files)
        infilelist.append(file)

    return infilelist

def process_infile(file: str) -> list:
    # Step 3: read config file
    logger.info(f'###### Step3 - Load json config from {file}')
    [devices, req_tables, remove_properties_flag] = read_config_json(os.path.join(CONFIG_DIR_FULL, file))

    outfilelist = []
    # Process each device in the config
    for idx, device in enumerate(devices):
        logger.info(f'###### Step4 - Processing device {idx+1}/{len(devices)}: {device["environment"]}')

        # Step 4: login apic
        logger.info(f'###### Step4 - Login apic and get token for {device["environment"]}:')
        token = get_f5ltm_token(device['ip'], device['username'], device['password'])

        # Step 5: process apic api and export to excel
        # Prepare excel writer
        outfile = f"apic_{device['environment']}_{get_datetime()}.xlsx"
        writer = pd.ExcelWriter(os.path.join(PARENT_DIR, outfile))
        logger.info(f'###### Step5 - Start processing, export to {outfile}')

        for i in range(len(req_tables)):
            logger.info(f" ### [{i + 1}/{len(req_tables)}], process {req_tables[i]['key']}")
            # Step 5A: Get api resp
            resp = get_f5ltm_api_resp(device['ip'], req_tables[i]['key'], token)

            # Step 5B: Export to df
            df1 = parse_f5ltm_json(resp.json(), req_tables[i]['key'])

            # Step 5C: remove properties column
            if remove_properties_flag == 1:
                df1 = remove_columns(df1, req_tables[i]['remove_properties'])

            # Step 5D: export to excel
            export_df_to_xlsx(writer, df1, req_tables[i]['key'])

        logger.info(f'###')
        logger.info(f'###')
        logger.info(f'### close output: {outfile}')
        logger.info(f'###')
        logger.info(f'###')
        writer.close()
        outfilelist.append(outfile)

    return outfilelist

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--infiles", help="input json in config folder, example: -i all_apic_example.json")
    parser.add_argument("-a", "--anaylsis", action='store_true', help="flag to analysis and parse table to new excel")
    args = parser.parse_args()

    outfilelist = start_script(args)

    if args.anaylsis:
        logger.info(f'######  ')
        logger.info(f'###### anaylsis argument enabled, analysis and parse table to new excel')
        logger.info(f'######  ')
        for file in outfilelist:
            args2 = argparse.Namespace()
            args2.infiles = file
            pyapicanaylsis_interface.start_script(args2)
            pyapicanaylsis_contract.start_script(args2)

    logger.info(f'##################         END SCRIPT       ################## ')
    logger.info(f'############################################################## ')

if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    main()