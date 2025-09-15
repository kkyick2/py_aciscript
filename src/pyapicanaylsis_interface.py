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
        file = prompt_select_file(files)
        infilelist.append(file)

    return infilelist

def df_intf_profile_split_row(row):
    # example:
    # Raw
    # dn	_nodeid	_intf_p	_intf_n	_policyGrp
    # uni/infra/accportprof-lif-1101/hports-p31-typ-range	1101	p31	31	ipg-vm
    # uni/infra/accportprof-lif-1101/hports-p32-typ-range	1101	p32	32	ipg-vm
    # uni/infra/accportprof-lif-1103-1104/hports-p3-typ-range	1103-1104	p3	3	vpc-leaf1103-1104-p3
    # uni/infra/accportprof-lif-1103/hports-p1-p2-typ-range	1103	p1-p2	1-2	ipg-inb
    # uni/infra/accportprof-lif-1201/hports-p16-17-typ-range	1201	p16-17	16-17	ipg-phy
    # Result
    # dn	_nodeid	_intf_p	_intf_n	_policyGrp
    # uni/infra/accportprof-lif-1101/hports-p31-typ-range	1101	p31	31	ipg-vm
    # uni/infra/accportprof-lif-1101/hports-p32-typ-range	1101	p32	32	ipg-vm
    # uni/infra/accportprof-lif-1103-1104/hports-p3-typ-range	1103	p3	3	vpc-leaf1103-1104-p3
    # uni/infra/accportprof-lif-1103-1104/hports-p3-typ-range	1104	p3	3	vpc-leaf1103-1104-p3
    # uni/infra/accportprof-lif-1103/hports-p1-p2-typ-range	1103	p1	1	ipg-inb
    # uni/infra/accportprof-lif-1103/hports-p1-p2-typ-range	1103	p2	2	ipg-inb
    # uni/infra/accportprof-lif-1201/hports-p16-17-typ-range	1201	p16	16	ipg-phy
    # uni/infra/accportprof-lif-1201/hports-p16-17-typ-range	1201	p17	17	ipg-phy

    # Handle nodeid ranges (e.g., '1103-1104')
    nodeids = []
    if '-' in row['_nodeid']:
        start, end = row['_nodeid'].split('-')
        nodeids = [str(i) for i in range(int(start), int(end) + 1)]
    else:
        nodeids = [row['_nodeid']]

    # Handle interface ranges (e.g., 'p1-p2' or 'p16-17')
    intf_ps = []
    intf_ns = []
    if '-' in row['_intf_p']:
        prefix = row['_intf_p'][0]  # 'p' or other prefix
        start, end = map(int, row['_intf_n'].split('-'))
        intf_ps = [f"{prefix}{i}" for i in range(start, end + 1)]
        intf_ns = [str(i) for i in range(start, end + 1)]
    else:
        intf_ps = [row['_intf_p']]
        intf_ns = [row['_intf_n']]

    # Create rows for each combination of nodeid and interface
    result = []
    for nodeid in nodeids:
        for intf_p, intf_n in zip(intf_ps, intf_ns):
            result.append({
                'dn': row['dn'],
                '_nodeid': nodeid,
                '_intf_p': intf_p,
                '_intf_n': intf_n,
                '_policyGrp': row['_policyGrp']
            })
    return result

def process_infile(file: str) -> None:
    # step 3: column operation
    # ===========================================================================
    logger.info(f'###### Step3 - Process excel: {file}')
    # step 3A: Get system
    # topSystem ========================================
    df_topSystem = pd.read_excel(file, sheet_name='topSystem')
    df_topSystem = df_topSystem[['dn', 'name', 'id', 'fabricId', 'podId', 'role', 'serial', 'state', 'version', 'oobMgmtAddr',
                                 'inbMgmtAddr', 'inbMgmtGateway', 'lastRebootTime', 'lastResetReason', 'systemUpTime', 'tepPool', 'address']]  # choose column
    df_topSystem = df_topSystem.sort_values(by=['id'])

    # step 3C: Get transceiver sfp_sn
    # ethpmFcot ========================================
    df_ethpmFcot = pd.read_excel(file, sheet_name='ethpmFcot')
    df_ethpmFcot = df_ethpmFcot[['dn', 'guiCiscoEID', 'guiName', 'guiSN']]  # choose column
    df_ethpmFcot['dn'] = df_ethpmFcot['dn'].str.replace(r'/phys/fcot$', '', regex=True)

    # step 3B: Get interface l1PhysIf
    # l1PhysIf ========================================
    df_l1PhysIf = pd.read_excel(file, sheet_name='l1PhysIf')
    df_l1PhysIf = df_l1PhysIf[['dn', 'id', 'descr', 'portT', 'mode', 'layer', 'usage', 'adminSt', 'autoNeg']]  # choose column
    df_l1PhysIf = df_l1PhysIf.sort_values(by=['dn'])

    # step 3B: Get interface ethpmPhysIf
    # ethpmPhysIf ========================================
    df_ethpmPhysIf = pd.read_excel(file, sheet_name='ethpmPhysIf')
    df_ethpmPhysIf = df_ethpmPhysIf[['dn','lastLinkStChg','nativeVlan', 'operSpeed', 'operDuplex','operSt', 'operStQual', 'bundleIndex', 'operVlans']]  # choose column
    df_ethpmPhysIf['dn'] = df_ethpmPhysIf['dn'].str.replace(r'/phys$', '', regex=True)
    df_ethpmPhysIf = df_ethpmPhysIf.sort_values(by=['dn'])
    
    # merge (df_l1PhysIf <- df_ethpmPhysIf)
    df_interface =  pd.merge(df_l1PhysIf, df_ethpmPhysIf, on="dn", how="left")
    # merge (df_interface <- df_ethpmFcot)
    df_interface =  pd.merge(df_interface, df_ethpmFcot, on="dn", how="left")

    # column index update
    # _nodeid, dn = topology/pod-1/node-1101/sys/phys-[eth1/10] -> 1101
    df_interface['_nodeid'] = df_interface['dn'].str.replace(r'.*node-(\d+).*', r'\1', regex=True)
    # _intf, id = eth1/10 -> eth1/10
    df_interface['_intf'] = df_interface['id']
    # _intf_p, dn = topology/pod-1/node-1101/sys/phys-[eth1/10] -> p10
    df_interface['_intf_p'] = df_interface['dn'].str.replace(r'.*\[eth1\/(\d+)\].*', 'p'+ r'\1', regex=True)
    # _intf_n, _intf_p = p10 -> 10
    df_interface['_intf_n'] = df_interface['_intf_p'].str.replace(r'[pP](\d+(?:-\d+)?)', r'\1', regex=True)
    df_interface = df_interface[['dn','_nodeid' ,'_intf','_intf_p','_intf_n', 'descr', 'portT', 'layer', 'usage', 'operSpeed','operDuplex', 'autoNeg', 'adminSt','operSt', 'operStQual', 'guiCiscoEID', 'bundleIndex', 'operVlans', 'nativeVlan', 'lastLinkStChg']] # choose column

    # step 3D,3E,3F: Get encp-all, epg-encp, intf-encp, 
    # fvRsPathAtt ========================================
    df_fvRsPathAtt = pd.read_excel(file, sheet_name='fvRsPathAtt')
    df_fvRsPathAtt = df_fvRsPathAtt[['dn', 'encap', 'instrImedcy' ,'mode' ,'tDn']]  # choose column
    df_fvRsPathAtt['dn'] = df_fvRsPathAtt['dn'].str.replace(r'/rspathAtt-\[topology/(.*)\]\]$', '', regex=True)
    df_fvRsPathAtt['encap'] = df_fvRsPathAtt['encap'].str.replace(r'^vlan-', '', regex=True)
    # For output [all, encap]
    df_all_encap = df_fvRsPathAtt[['dn', 'encap', 'instrImedcy' ,'mode' ,'tDn']] # choose column
    
    # For output [epg, encap]
    df_epg_encap = df_fvRsPathAtt[['dn','encap']]
    df_epg_encap = df_epg_encap.sort_values(by=['encap'])
    df_epg_encap = df_epg_encap.groupby('dn')['encap'].agg(lambda col: ','.join(col.unique())).reset_index()    # group

    # For output [intf, encap]
    df_intf_encap = df_fvRsPathAtt[['tDn','encap']]
    df_intf_encap = df_intf_encap.sort_values(by=['encap'])
    df_intf_encap = df_intf_encap.groupby('tDn')['encap'].agg(lambda col: ','.join(col.unique())).reset_index()    # group
    df_intf_encap = df_intf_encap.sort_values(by=['tDn'])

    # step 3G: Get leaf vlan_encap
    # vlanCktEp ========================================
    df_vlanCktEp = pd.read_excel(file, sheet_name='vlanCktEp')
    df_vlanCktEp = df_vlanCktEp[['ctrl','dn', 'encap', 'epgDn' ,'fabEncap' ,'id', 'pcTag']]  # choose column
    df_leaf_vlan_encap = df_vlanCktEp[['dn','epgDn' ,'ctrl','encap','fabEncap','id','pcTag']]  # choose column

    # step 3H: Get interface profile
    # infraRsAccBaseGrp ========================================
    df_infraRsAccBaseGrp = pd.read_excel(file, sheet_name='infraRsAccBaseGrp')
    df_intf_profile = df_infraRsAccBaseGrp[['dn', 'tCl', 'tDn']] # choose column
    # Always create an explicit .copy() when you intend to work on a subset of a DataFrame independently.
    df_intf_profile = df_intf_profile.copy()
    df_intf_profile.loc[:, 'dn'] = df_intf_profile['dn'].str.replace(r'/rsaccBaseGrp$', '', regex=True)
    # _nodeid, dn = uni/infra/accportprof-lif-1101-1102/hports-p48-typ-range -> 1101-1102
    df_intf_profile.loc[:, '_nodeid'] = df_intf_profile['dn'].str.replace(r'.*accportprof-lif-(\d+(?:-\d+)?)\/hports.*', r'\1', regex=True)
    # _intf_p, dn = uni/infra/accportprof-lif-1101-1102/hports-p48-typ-range -> p48
    # _intf_p, dn = uni/infra/accportprof-lif-1104/hports-p1-p2-typ-range -> p1-p2
    # _intf_p, dn = uni/infra/accportprof-lif-1201/hports-p16-17-typ-range -> p16-17
    df_intf_profile.loc[:, '_intf_p'] = df_intf_profile['dn'].str.replace(r'.*\/hports-([pP]\d+(?:-\w+)?)-typ-range.*', r'\1', regex=True)
    # _intf_n, _intf_p = p10 -> 10
    df_intf_profile.loc[:, '_intf_n'] = df_intf_profile['_intf_p'].str.replace(r'[pP](\d+(?:-\d+)?)', r'\1', regex=True)
    # _policyGrp, tDn = uni/infra/funcprof/accbundle-vpc-leaf1101-1102-p44 -> vpc-leaf1101-1102-p44
    # _policyGrp, tDn = uni/infra/funcprof/accportgrp-ipg-vm -> ipg-vm
    df_intf_profile.loc[:, '_policyGrp'] = df_intf_profile['tDn'].str.replace(r'uni\/infra\/funcprof\/(?:accportgrp-|accbundle-)(.*)$', r'\1', regex=True)
    df_intf_profile = df_intf_profile.sort_values(by=['dn'])
    df_intf_profile = df_intf_profile[['dn', '_nodeid','_intf_p', '_intf_n', '_policyGrp']]

    # step 3H part2: df_intf_profile split row
    # ========================================
    # Apply the function and create new DataFrame
    expanded_rows = []
    for _, row in df_intf_profile.iterrows():
        expanded_rows.extend(df_intf_profile_split_row(row))
    # Create final DataFrame
    df_intf_profile_split = pd.DataFrame(expanded_rows)
    # Sort by _nodeid and _intf_n for consistency
    df_intf_profile_split = df_intf_profile_split.sort_values(['_nodeid', '_intf_n']).reset_index(drop=True)


    # step 3I: Get port channel / vpc profile
    # infraAccBndlGrp ========================================
    df_infraAccBndlGrp = pd.read_excel(file, sheet_name='infraAccBndlGrp')
    df_vpc_profile = df_infraAccBndlGrp[['dn', 'name', 'descr']]
    df_vpc_profile = df_vpc_profile.sort_values(by=['dn'])

    # merge to interface (df_interface <- df_intf_profile_split)
    # ========================================
    df_interface =  pd.merge(df_interface, df_intf_profile_split, on=['_nodeid','_intf_p'] , how="left")
    df_interface = df_interface[['dn_x','_nodeid' ,'_intf','_intf_p','_intf_n_x', 'descr', 'portT', 'layer', 'usage', 'operSpeed','operDuplex', 'autoNeg', 'adminSt','operSt', 'operStQual', 'guiCiscoEID', 'bundleIndex', 'operVlans', 'nativeVlan', '_policyGrp', 'lastLinkStChg']] # choose column
    df_interface = df_interface.rename(columns={'dn_x': 'dn', '_intf_n_x': '_intf_n'})

    # step 99: export result to xlsx apic_n1_xxxx_20241016_1335.xlsx
    outfile_env = file.split("_")[1]
    outfile = f"apic_{outfile_env}_interface_{get_datetime()}.xlsx"
    writer = pd.ExcelWriter(os.path.join(PARENT_DIR, outfile))
    tshoot = 0
    export_df_to_xlsx(writer, df_topSystem, 'topSystem')
    export_df_to_xlsx(writer, df_interface, 'interface')
    export_df_to_xlsx(writer, df_ethpmFcot, 'sfp_sn')
    export_df_to_xlsx(writer, df_all_encap, 'all_encap')
    export_df_to_xlsx(writer, df_epg_encap, 'epg_encap')
    export_df_to_xlsx(writer, df_intf_encap, 'intf_encap')
    export_df_to_xlsx(writer, df_leaf_vlan_encap, 'leaf_encap')
    export_df_to_xlsx(writer, df_intf_profile_split, 'intf_prof_split')
    
    if tshoot == 1:
        export_df_to_xlsx(writer, df_l1PhysIf, 'l1PhysIf')
        export_df_to_xlsx(writer, df_ethpmPhysIf, 'ethpmPhysIf')
        export_df_to_xlsx(writer, df_fvRsPathAtt, 'fvRsPathAtt')
        export_df_to_xlsx(writer, df_intf_profile, 'intf_prof')
        export_df_to_xlsx(writer, df_vpc_profile, 'vpc_prof')

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