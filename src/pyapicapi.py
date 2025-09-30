import logging
import logging.config
import argparse, os, re, json, requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
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
    log_name = Path(os.path.basename(__file__)).stem
    log_config = log_configs.get(LOG_ENV, "logging.dev.json")
    log_config_path = os.path.join(PARENT_DIR, CONFIG_DIR, log_config)
    log_file_path = os.path.join(PARENT_DIR, LOG_DIR, f'{log_name}_{DATETIME}.log')
    
    with open(log_config_path, 'r') as f:
        config = json.load(f)
    
    for handler in config['handlers'].values():
        if handler['class'] == 'logging.FileHandler':
            handler['filename'] = log_file_path

    logging.config.dictConfig(config)
    return

def get_device_token(ip: str, username: str, password: str) -> str:
    url = f'https://{ip}/api/aaaLogin.json'
    payload = {
        "aaaUser": {
            "attributes": {
                "name": username,
                "pwd": password,
            }
        }
    }
    headers = {
        "Accept":"application/json",
        "Content-Type": "application/json"
    }
    requests.packages.urllib3.disable_warnings()
    try:
        resp = requests.post(url, headers=headers, data=json.dumps(payload), verify=False)
        resp.raise_for_status()
        token = resp.json()['imdata'][0]['aaaLogin']['attributes']['token']
        logger.info(f'Token obtained for {ip}')
        return token
    except requests.RequestException as e:
        logger.error(f'Failed to get token for {ip}: {str(e)}')
        raise

def get_device_api_resp(ip: str, key: str, token: str) -> requests.Response:
    url = f'https://{ip}/api/class/{key}.json'
    payload={}
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Cookie" : f'APIC-Cookie={token}'
    }
    try:
        logger.info(f'Fetching API data: {key} from {url}')
        resp = requests.get(url, headers=headers, data=payload, verify=False)
        resp.raise_for_status()
        return resp
    except requests.RequestException as e:
        logger.error(f'Failed to fetch API data for {key} from {ip}: {str(e)}')
        raise

def parse_device_json(json_obj: list, key: str) -> pd.DataFrame:
    parsed_data = []
    for data in json_obj['imdata']:
        parsed_data.append(data[key]['attributes'])
    df = pd.DataFrame(parsed_data)
    logger.debug(f'Exported to dataframe for {key}, size: {df.shape}')
    return df

def remove_columns(df: pd.DataFrame, properties: list) -> pd.DataFrame:
    for i in properties:
        if i in df.columns:
            df.pop(i)
    logger.debug(f'Removed properties, new size: {df.shape}')
    return df

def export_df_to_xlsx(writer: pd.ExcelWriter, df: pd.DataFrame, key: str) -> None:
    df.to_excel(writer, sheet_name=key, index=False)
    ws = writer.sheets[key]
    return

def get_config_files_to_list(dir: str) -> list:
    matched_files = []
    files = os.listdir(dir)
    for f in files:
        m = re.search("(.*)apic.*\.json", f)
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
    logger.info(f'Selected file: {selected_file}')
    return selected_file 

def read_config_json(in_file: str) -> list:
    try:
        with open(in_file, 'r') as f:
            config = json.load(f)
        
        if not config.get('devices'):
            raise ValueError("No 'devices' array found in config file")
        
        tables_file = os.path.join(CONFIG_DIR_FULL, config['devices'][0]['tables'])
        with open(tables_file, 'r') as f:
            tables_config = json.load(f)
        
        logger.info(f"Info collected, tables to process: {len(tables_config['tables'])} for {len(config['devices'])} devices")
        return [config['devices'], tables_config['tables'], tables_config['remove_properties_flag']]
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        logger.error(f'Error reading config file {in_file}: {str(e)}')
        raise

def process_device(device: dict, req_tables: list, remove_properties_flag: int, batch_datetime: str) -> str:
    try:
        logger.info(f'Processing device: {device["environment"]}')
        token = get_device_token(device['ip'], device['username'], device['password'])

        outfile = f"apic_{device['environment']}_{batch_datetime}.xlsx"
        writer = pd.ExcelWriter(os.path.join(PARENT_DIR, outfile))
        logger.info(f'Start processing, export to {outfile}')

        for i, table in enumerate(req_tables):
            logger.info(f"[{i + 1}/{len(req_tables)}], process {table['key']} for {device['environment']}")
            resp = get_device_api_resp(device['ip'], table['key'], token)
            df = parse_device_json(resp.json(), table['key'])
            if remove_properties_flag == 1:
                df = remove_columns(df, table['remove_properties'])
            export_df_to_xlsx(writer, df, table['key'])

        logger.info(f'Closing output: {outfile}')
        writer.close()
        return outfile
    except Exception as e:
        logger.error(f'Failed to process device {device["environment"]}: {str(e)}')
        raise

def process_analysis(outfile: str, batch_datetime: str) -> None:
    try:
        logger.info(f'Processing analysis for file: {outfile}')
        args = argparse.Namespace()
        args.infiles = outfile
        args.batch_datetime = batch_datetime
        pyapicanaylsis_interface.start_script(args)
        pyapicanaylsis_contract.start_script(args)
        logger.info(f'Successfully completed analysis for {outfile}')
    except Exception as e:
        logger.error(f'Failed to process analysis for {outfile}: {str(e)}')

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
        outf = process_infile(inf, args.batch_datetime)
        outfilelist.extend(outf)
    logger.info(f'###### Complete, outfiles: {outfilelist}')
    return outfilelist

def process_input(args) -> list:
    infilelist = []
    if args.infiles:
        logger.info(f'###### Step1 - Get config files from python arguments:')
        infilelist = args.infiles.split(',')

    if not args.infiles:
        logger.info(f'###### Step1 - Get config files in folder: {CONFIG_DIR_FULL}')
        files = get_config_files_to_list(CONFIG_DIR_FULL)
        logger.info(f'###### Step1 - Choose input config files from list:')
        file = prompt_select_file(files)
        infilelist.append(file)

    return infilelist

def process_infile(file: str, batch_datetime: str) -> list:
    logger.info(f'###### Step3 - Load json config from {file}')
    try:
        [devices, req_tables, remove_properties_flag] = read_config_json(os.path.join(CONFIG_DIR_FULL, file))
    except Exception as e:
        logger.error(f'Failed to process config file {file}: {str(e)}')
        return []

    outfilelist = []
    max_workers = min(len(devices), 4)
    logger.info(f'###### Step4 - Processing {len(devices)} devices concurrently with {max_workers} workers')

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_device = {executor.submit(process_device, device, req_tables, remove_properties_flag, batch_datetime): device for device in devices}
        for future in future_to_device:
            device = future_to_device[future]
            try:
                outfile = future.result()
                outfilelist.append(outfile)
                logger.info(f'Successfully processed device {device["environment"]}, output: {outfile}')
            except Exception as e:
                logger.error(f'Error processing device {device["environment"]}: {str(e)}')

    return outfilelist

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--infiles", help="input json in config folder, example: -i all_apic_example.json")
    parser.add_argument("-a", "--anaylsis", action='store_true', help="flag to analysis and parse table to new excel")
    args = parser.parse_args()

    batch_datetime = get_datetime()
    logger.info(f'Batch datetime set to: {batch_datetime}')
    args.batch_datetime = batch_datetime

    outfilelist = start_script(args)

    if args.anaylsis:
        logger.info(f'######')
        logger.info(f'###### Analysis argument enabled, processing {len(outfilelist)} files concurrently')
        logger.info(f'######')
        max_workers = min(len(outfilelist), 4)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_analysis, outfile, batch_datetime) for outfile in outfilelist]
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f'Error in analysis task: {str(e)}')

    logger.info(f'##################         END SCRIPT       ################## ')
    logger.info(f'############################################################## ')

if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    main()