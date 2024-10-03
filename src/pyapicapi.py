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

def setup_logging():
    """Load logging configuration"""
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

def get_apic_token(ip, username, password):
    logger.info(f'### Step4 - Login apic get token:')
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
    resp = requests.post(url, headers=headers, data=json.dumps(payload), verify=False)
    token = resp.json()['imdata'][0]['aaaLogin']['attributes']['token']

    logger.info(f'Token: {token}')
    return token


def get_apic_api_resp(ip, key, token):
    url = f'https://{ip}/api/class/{key}.json'
    payload={}
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Cookie" : f'APIC-Cookie={token}'
    }

    logger.info(f'### Step5 - Login apic restapi: {key} - {url}')
    resp = requests.get(url, headers=headers, data=payload, verify=False)
    return resp

def parse_apic_json_to_df(key, json_obj):
    # parse apic json to dataframe
    #               lv1        lv2(key)       lv3
    # json_obj = {'imdata': [{'fabricPod': {'attributes': {'childAction ...  || <class 'dict'>
    # data_list = json_obj[imdata] = {'fabricPod': {'attributes': {'childAction ...}, <class 'list'>
    #                                {'fabricPod': {'attributes': {'childAction ...},
    lv1 = 'imdata'
    lv2 = key
    lv3 = 'attributes'
    parsed_data = []
    data_list = json_obj[lv1] 
    for data in data_list:
        parsed_data.append(data[lv2][lv3])
    df = pd.DataFrame(parsed_data)
    logger.info(f' Table size: {df.shape}')
    return df

def get_config_files_to_list(dir):
    logger.info(f'### Step1 - Get config files in: {dir}')
    matched_files = []
    files = os.listdir(dir)
    for f in files:
        m = re.search("(.*).txt",f)
        if m:
            matched_files.append(f)
    return matched_files

def promt_user_select_file(files):
    logger.info(f'### Step2 - Choose input config files from list:')
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

def get_datetime():
    return datetime.now().strftime("%Y%m%d_%H%M")

def export_df_to_xlsx(writer, df, key):
    df.to_excel(writer, sheet_name=key, index=False)
    ws = writer.sheets[key]
    return

def read_config_file(in_file):
    logger.info(f'### Step3 - Loading details from {in_file}:')
    description = []
    table = []
    sheetname = []
    properties = []
    with open(in_file,'r') as f:
        for line in f:
            line = line.rstrip("\n\r")
            if (re.match("^#",line)):
                continue
            m = re.search("\$login=\"(.*)\"",line)
            if m:
                username = m.group(1)
                continue
            m = re.search("\$APIC_IP=\"(.*)\"",line)
            if m:
                ip = m.group(1)
                continue
            m = re.search("\$password=\"(.*)\"",line)
            if m:
                password = m.group(1)
                continue
            m = re.search("\$Environment=\"(.*)\"",line)
            if m:
                environment = m.group(1)
                continue
            m = re.search("\$remove_properties=(.*)",line)
            if m:
                remove_properties_flag = int(m.group(1))
                continue
            m = re.search("^\"",line)
            if m:
                list = line.split(",")
                description.append(list[0].replace('"',""))
                table.append(list[1].replace('"',""))
                sheetname.append(list[2].replace('"',""))
            elif re.search("^\'",line):
                line = line.replace(" ","")
                properties.append(line)
    login_info = {
        'ip': ip,
        'username': username,
        'password':  password,
        'environment': environment,
        'remove_properties' : remove_properties_flag
    }
    return [login_info, description, table, sheetname, properties]

def process_script():

    config_dir = os.path.join(PARENT_DIR, CONFIG_DIR)
    # step 1: get config files
    files = get_config_files_to_list(config_dir)

    # step 2: user select config file
    file = promt_user_select_file(files)

    # step 3: read config file
    [login_info, description, table, sheetname, properties] = read_config_file(os.path.join(config_dir, file))

    # step 4: login apic
    token = get_apic_token(login_info['ip'], login_info['username'], login_info['password'])

    # step 5: get apic data and export to excel
    outfile = 'result_' + get_datetime() + '.xlsx'
    writer = pd.ExcelWriter(os.path.join(PARENT_DIR, outfile))
    for i in range(len(table)):
        # Get apic restapi resp
        resp = get_apic_api_resp(login_info['ip'], table[i], token)
        df1 = parse_apic_json_to_df(table[i], resp.json())
        export_df_to_xlsx(writer, df1, table[i])
    writer.close()

if __name__ == "__main__":

    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info(f'############################################################## ')
    logger.info(f'##################       START SCRIPT       ################## ')

    process_script()

    logger.info(f'##################         END SCRIPT       ################## ')
    logger.info(f'############################################################## ')