import logging
import logging.config
import os, re, json, requests
import pandas as pd
from datetime import datetime
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

def get_apic_token(ip: str, username: str, password: str) -> str:
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

    logger.info(f'###### Step4 - Login apic and get token:')
    requests.packages.urllib3.disable_warnings()
    resp = requests.post(url, headers=headers, data=json.dumps(payload), verify=False)
    token = resp.json()['imdata'][0]['aaaLogin']['attributes']['token']

    logger.info(f'Token: {token}')
    return token

def get_apic_api_resp(ip: str, key: str, token: str) -> requests.Response:
    url = f'https://{ip}/api/class/{key}.json'
    payload={}
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Cookie" : f'APIC-Cookie={token}'
    }

    logger.info(f' Step5A - Login apic api: {key} - {url}')
    resp = requests.get(url, headers=headers, data=payload, verify=False)
    return resp

def parse_apic_json_to_df(key: str, json_obj: list) -> pd.DataFrame:
    # parse apic json to dataframe
    #               lv1        lv2(key)       lv3
    # json_obj = {'imdata': [{'fabricPod': {'attributes': {'childAction ...  || <class 'dict'>
    # data_list = json_obj[imdata] = {'fabricPod': {'attributes': {'childAction ...}, <class 'list'>
    #                                {'fabricPod': {'attributes': {'childAction ...},
    parsed_data = []
    for data in json_obj['imdata']:
        parsed_data.append(data[key]['attributes'])
    df = pd.DataFrame(parsed_data)
    logger.info(f' Step5B - Exported to dataframe, size: {df.shape}')
    return df

def remove_columns(df: pd.DataFrame, properties: list) -> pd.DataFrame:
    for i in properties:
        df.pop(i)
    logger.info(f' Step5C - Removed properties size: {df.shape}')
    return df

def export_df_to_xlsx(writer: pd.ExcelWriter, df: pd.DataFrame, key: str) -> None:
    df.to_excel(writer, sheet_name=key, index=False)
    ws = writer.sheets[key]
    return

def get_config_files_to_list(dir: str) -> list:
    logger.info(f'###### Step1 - Get config files in: {dir}')
    matched_files = []
    files = os.listdir(dir)
    for f in files:
        m = re.search("(.*)input.json",f)
        if m:
            matched_files.append(f)
    return matched_files

def promt_user_select_file(files: list) -> str:
    logger.info(f'###### Step2 - Choose input config files from list:')
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

def read_config_json(in_file: str) -> list:
    logger.info(f'###### Step3 - Load json config from {in_file}')
    with open(in_file,'r') as f:
        config = json.load(f)
    logger.info(f" Info collected, tables to process: {len(config['tables'])}")
    return [config['login'], config['tables']]

def process_script() -> None:
    config_dir = os.path.join(PARENT_DIR, CONFIG_DIR)

    # step 1: get config files
    files = get_config_files_to_list(config_dir)

    # step 2: user select config file
    file = promt_user_select_file(files)

    # step 3: read config file
    [login_info, req_tables] = read_config_json(os.path.join(config_dir, file))

    # step 4: login apic
    token = get_apic_token(login_info['ip'], login_info['username'], login_info['password'])

    # Prepare excel writer
    outfile = f"apic_{login_info['environment']}_{get_datetime()}.xlsx"
    writer = pd.ExcelWriter(os.path.join(PARENT_DIR, outfile))
    logger.info(f'###### Step5 - Start processing, export to {outfile}')

    for i in range(len(req_tables)):
        logger.info(f" ### [{i + 1}/{len(req_tables)}], process {req_tables[i]['key']}")
        # step 5A: Get apic api
        resp = get_apic_api_resp(login_info['ip'], req_tables[i]['key'], token)
        # step 5B: Export to df
        df1 = parse_apic_json_to_df(req_tables[i]['key'], resp.json())
        # step 5C: remove properties column
        if login_info['remove_properties_flag'] == 1:
            df1 = remove_columns(df1, req_tables[i]['remove_properties'])
        # step 5D: export to excel
        export_df_to_xlsx(writer, df1, req_tables[i]['key'])
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