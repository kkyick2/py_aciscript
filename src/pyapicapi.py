import logging
import logging.config
import argparse, os, re, json, requests, time
import pandas as pd
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod
import pyapicanaylsis_interface
import pyapicanaylsis_contract
verion = '20251009'
PARENT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
DATETIME = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_ENV = 'dev'
LOG_DIR = 'log'
CONFIG_DIR = 'config'
CONFIG_DIR_FULL = os.path.join(PARENT_DIR, CONFIG_DIR)

logger = logging.getLogger(__name__)

# Device registry to map device types to their classes
DEVICE_REGISTRY = {}

def register_device_type(cls):
    """Decorator to register device type classes."""
    DEVICE_REGISTRY[cls.device_type] = cls
    return cls

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
    
    try:
        with open(log_config_path, 'r') as f:
            config = json.load(f)
        
        for handler in config['handlers'].values():
            if handler['class'] == 'logging.FileHandler':
                handler['filename'] = log_file_path

        logging.config.dictConfig(config)
    except Exception as e:
        logging.basicConfig(level=logging.INFO)
        logger.error(f"Failed to load logging config: {str(e)}")
    return

# Abstract base class for device types
class DeviceBaseClass(ABC):
    device_type = None

    def __init__(self, ip: str, username: str, password: str):
        self.ip = ip
        self.username = username
        self.password = password

    @abstractmethod
    def get_token(self) -> str:
        pass

    @abstractmethod
    def get_api_resp(self, key: str, token: str) -> requests.Response:
        pass

    @abstractmethod
    def parse_json(self, json_obj: dict, key: str) -> pd.DataFrame:
        pass

@register_device_type
class CiscoApicDevice(DeviceBaseClass):
    device_type = "cisco_apic"

    def get_token(self) -> str:
        url = f'https://{self.ip}/api/aaaLogin.json'
        payload = {
            "aaaUser": {
                "attributes": {
                    "name": self.username,
                    "pwd": self.password,
                }
            }
        }
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        requests.packages.urllib3.disable_warnings()
        try:
            resp = requests.post(url, headers=headers, data=json.dumps(payload), verify=False)
            resp.raise_for_status()
            token = resp.json()['imdata'][0]['aaaLogin']['attributes']['token']
            logger.info(f'Token obtained for {self.ip}')
            return token
        except requests.RequestException as e:
            logger.error(f'Failed to get token for {self.ip}: {str(e)}')
            raise

    def get_api_resp(self, key: str, token: str) -> requests.Response:
        url = f'https://{self.ip}/api/class/{key}.json'
        payload = {}
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Cookie": f'APIC-Cookie={token}'
        }
        try:
            logger.info(f'Fetching API data: {key} from {url}')
            resp = requests.get(url, headers=headers, data=payload, verify=False)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            logger.error(f'Failed to fetch API data for {key} from {self.ip}: {str(e)}')
            raise

    def parse_json(self, json_obj: dict, key: str) -> pd.DataFrame:
        parsed_data = []
        try:
            for data in json_obj['imdata']:
                parsed_data.append(data[key]['attributes'])
            df = pd.DataFrame(parsed_data)
            logger.debug(f'Exported to dataframe for {key}, size: {df.shape}')
            return df
        except KeyError as e:
            logger.error(f'Failed to parse JSON for {key}: {str(e)}')
            raise

@register_device_type
class CiscoNexusDevice(DeviceBaseClass):
    device_type = "cisco_nexus"

    def get_token(self) -> str:
        raise NotImplementedError("Cisco Nexus token retrieval not implemented")

    def get_api_resp(self, key: str, token: str) -> requests.Response:
        raise NotImplementedError("Cisco Nexus API response not implemented")

    def parse_json(self, json_obj: dict, key: str) -> pd.DataFrame:
        raise NotImplementedError("Cisco Nexus JSON parsing not implemented")

@register_device_type
class F5LtmDevice(DeviceBaseClass):
    device_type = "f5_ltm"

    def get_token(self) -> str:
        url = f'https://{self.ip}/mgmt/shared/authn/login'
        payload = {
            "username": self.username,
            "password": self.password,
            "loginProviderName": "tmos"
        }
        headers = {
            "Accept":"application/json",
            "Content-Type": "application/json"
        }
        requests.packages.urllib3.disable_warnings()
        try:
            resp = requests.post(url, headers=headers, data=json.dumps(payload), verify=False)
            resp.raise_for_status()
            token = resp.json()['token']['token']
            logger.info(f'Token obtained for {self.ip}')
            return token
        except requests.RequestException as e:
            logger.error(f'Failed to get token for {self.ip}: {str(e)}')
            raise

    def get_api_resp(self, key: str, token: str) -> requests.Response:
        url = f'https://{self.ip}/mgmt/tm/ltm/{key}'
        payload={}
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-F5-Auth-Token" : f'{token}'
        }
        try:
            logger.info(f'Fetching API data: {key} from {url}')
            resp = requests.get(url, headers=headers, data=payload, verify=False)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            logger.error(f'Failed to fetch API data for {key} from {self.ip}: {str(e)}')
            raise

    def parse_json(self, json_obj: dict, key: str) -> pd.DataFrame:
        parsed_data = []
        try:
            for data in json_obj['items']:
                parsed_data.append(data)
            df = pd.DataFrame(parsed_data)
            logger.debug(f'Exported to dataframe for {key}, size: {df.shape}')
            return df
        except KeyError as e:
            logger.error(f'Failed to parse JSON for {key}: {str(e)}')
            raise

# Analysis registry: Maps device_type to list of analysis scripts
ANALYSIS_REGISTRY = {
    "cisco_apic": [pyapicanaylsis_interface, pyapicanaylsis_contract],
    "cisco_nexus": [],
    "f5_ltm": [],
}

def remove_columns(df: pd.DataFrame, properties: list) -> pd.DataFrame:
    for i in properties:
        if i in df.columns:
            df.pop(i)
    logger.debug(f'Removed properties, new size: {df.shape}')
    return df

def export_df_to_xlsx(writer: pd.ExcelWriter, df: pd.DataFrame, key: str) -> None:
    try:
        df.to_excel(writer, sheet_name=key, index=False)
        ws = writer.sheets[key]
    except Exception as e:
        logger.error(f'Failed to export DataFrame to Excel for sheet {key}: {str(e)}')
        raise
    return

def get_config_files_to_list(dir: str) -> list:
    matched_files = []
    try:
        files = os.listdir(dir)
        for f in files:
            m = re.search("(.*)apic|ltm(.*).json", f)
            if m:
                matched_files.append(f)
    except OSError as e:
        logger.error(f"Failed to list config files in {dir}: {str(e)}")
        raise
    return matched_files

def prompt_select_file(files: list) -> str:
    if not files:
        logger.error("No config files found in the config directory")
        raise ValueError("No config files found")
    if len(files) == 1:
        logger.info(f' 0) {files[0]}')
        selected_file = files[0]
    else:
        for i in range(len(files)):
            logger.info(f' {str(i)}) {files[i]}')
        print("---------------------------------------------\n")
        try:
            x = int(input("Pick input file from the list above: "))
            print("\n---------------------------------------------")
            selected_file = files[x]
        except (ValueError, IndexError) as e:
            logger.error(f"Invalid file selection: {str(e)}")
            raise ValueError("Invalid file selection")
    logger.info(f'Selected file: {selected_file}')
    return selected_file 

def read_config_json(in_file: str) -> tuple:
    try:
        with open(in_file, 'r') as f:
            config = json.load(f)
        
        if not config.get('devices'):
            raise ValueError("No 'devices' array found in config file")
        
        tables_file = os.path.join(CONFIG_DIR_FULL, config['devices'][0]['tables'])
        with open(tables_file, 'r') as f:
            tables_config = json.load(f)
        
        logger.info(f"Info collected, tables to process: {len(tables_config['tables'])} for {len(config['devices'])} devices")
        return (config['devices'], tables_config['tables'], tables_config['remove_properties_flag'])
    except Exception as e:
        logger.error(f'Failed to read config file {in_file}: {str(e)}')
        raise

def process_device(device: dict, req_tables: list, remove_properties_flag: int, batch_datetime: str) -> str:
    try:
        if 'device_type' not in device:
            logger.error(f"Device {device['environment']} missing 'device_type' in configuration")
            raise ValueError("Missing 'device_type' in device configuration")
        device_type = device['device_type']
        if device_type not in DEVICE_REGISTRY:
            logger.error(f"Unsupported device type: {device_type}")
            raise ValueError(f"Unsupported device type: {device_type}")
        
        device_handler = DEVICE_REGISTRY[device_type](device['ip'], device['username'], device['password'])
        logger.info(f'###### Step4 - Login device and get token for {device["environment"]} ({device_type}):')
        token = device_handler.get_token()
        
        outfile = f"{device['device_type'].replace('_', '')}_{device['environment']}_{batch_datetime}.xlsx"
        writer = pd.ExcelWriter(os.path.join(PARENT_DIR, outfile))
        
        for table in req_tables:
            logger.info(f"### [{req_tables.index(table) + 1}/{len(req_tables)}], process {table['key']}")
            resp = device_handler.get_api_resp(table['key'], token)
            df = device_handler.parse_json(resp.json(), table['key'])
            if remove_properties_flag == 1 and 'remove_properties' in table:
                df = remove_columns(df, table['remove_properties'])
            export_df_to_xlsx(writer, df, table['key'])
        
        logger.info(f'Closing output: {outfile}')
        writer.close()
        return outfile
    except Exception as e:
        logger.error(f'Failed to process device {device["environment"]}: {str(e)}')
        raise

def process_analysis(outfile: str, device_type: str, batch_datetime: str) -> None:
    try:
        logger.info(f'Processing analysis for file: {outfile} (device_type: {device_type})')
        if device_type not in ANALYSIS_REGISTRY:
            logger.warning(f'No analysis scripts defined for device type: {device_type}. Skipping analysis.')
            return
        args = argparse.Namespace()
        args.infiles = outfile
        args.batch_datetime = batch_datetime
        for analysis_module in ANALYSIS_REGISTRY[device_type]:
            analysis_module.start_script(args)
        logger.info(f'Successfully completed analysis for {outfile}')
    except Exception as e:
        logger.error(f'Failed to process analysis for {outfile}: {str(e)}')

def start_script(args) -> tuple:
    logger.info(f'######')
    logger.info(f'######')
    logger.info(f'############################################################## ')
    logger.info(f'##################       START SCRIPT       ################## ')
    logger.info(f'###### Step1')
    infilelist = process_input(args)
    outfilelist = []
    device_configs = {}

    logger.info(f'###### Step2 - Process config files: {infilelist}')
    i = 0
    for inf in infilelist:
        logger.info(f'###### {i+1}/{len(infilelist)}, process {inf}')
        i += 1
        outf, devices = process_infile(inf, args.batch_datetime)
        outfilelist.extend(outf)
        for device in devices:
            device_configs[f"{device['device_type'].replace('_', '')}_{device['environment']}_{args.batch_datetime}.xlsx"] = device['device_type']
    logger.info(f'###### Complete, outfiles: {outfilelist}, device_config: {device_configs}')
    return outfilelist, device_configs

def process_input(args) -> list:
    infilelist = []
    if args.infiles:
        logger.info(f'###### Step1 - Get config files from python arguments:')
        infilelist = args.infiles.split(',')
    else:
        logger.info(f'###### Step1 - Get config files in folder: {CONFIG_DIR_FULL}')
        files = get_config_files_to_list(CONFIG_DIR_FULL)
        logger.info(f'###### Step1 - Choose input config files from list:')
        file = prompt_select_file(files)
        infilelist.append(file)
    return infilelist

def process_infile(file: str, batch_datetime: str) -> tuple:
    logger.info(f'###### Step3 - Load json config from {file}')
    try:
        devices, req_tables, remove_properties_flag = read_config_json(os.path.join(CONFIG_DIR_FULL, file))
    except Exception as e:
        logger.error(f'Failed to process config file {file}: {str(e)}')
        return [], []

    outfilelist = []
    max_workers = max(1, min(len(devices), 4))
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
                continue

    return outfilelist, devices

def main():
    start_time = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--infiles", help="input json in config folder, example: -i all_apic_example.json")
    parser.add_argument("-a", "--anaylsis", action='store_true', help="flag to analysis and parse table to new excel")
    args = parser.parse_args()

    batch_datetime = get_datetime()
    logger.info(f'Batch datetime set to: {batch_datetime}')
    args.batch_datetime = batch_datetime

    outfilelist, device_configs = start_script(args)

    if args.anaylsis:
        logger.info(f'######')
        logger.info(f'###### Analysis argument enabled, processing {len(outfilelist)} files')
        logger.info(f'######')
        if outfilelist:
            max_workers = max(1, min(len(outfilelist), 4))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(process_analysis, outfile, device_configs.get(outfile, "unknown"), batch_datetime) for outfile in outfilelist]
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f'Error in analysis task: {str(e)}')
        else:
            logger.warning('No output files to analyze. Skipping analysis.')

    logger.info(f'##################         END SCRIPT       ################## ')
    logger.info(f'############################################################## ')
    print("---script run time: %s seconds ---" % (time.time() - start_time))

if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    main()