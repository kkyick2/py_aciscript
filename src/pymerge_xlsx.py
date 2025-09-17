import pandas as pd
import os
import time
from collections import defaultdict

def extract_prefix(filename):
    """ Extracts environment prefix from filename ('apic_env_datatype_yyyymmdd.xlsx') """
    base = os.path.basename(filename)
    parts = base.split('_')
    # e.g., ['apic', 'dev1', 'name', 'yyyymmdd.xlsx']
    return parts[1] if len(parts) >= 4 else ''

def extract_apic_and_datatype(filenames):
    """Extract apic and data type for output filename (from any input file)"""
    base = os.path.basename(filenames[0])
    parts = base.split('_')
    apic = parts[0] if len(parts) >= 4 else 'merged'
    datatype = parts[2] if len(parts) >= 4 else 'data'
    return apic, datatype

def merge_xlsx_files(filenames):
    # Mapping: sheet_name -> list of dataframes (with prefix column)
    sheets_data = defaultdict(list)

    for file in filenames:
        prefix = extract_prefix(file)
        xls = pd.ExcelFile(file)
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name)
            df.insert(0, 'prefix', prefix)
            sheets_data[sheet_name].append(df)
    
    # Merged sheets with the same sheet_name
    merged_sheets = {}
    for sheet_name, dfs in sheets_data.items():
        merged_sheets[sheet_name] = pd.concat(dfs, ignore_index=True)
    
    return merged_sheets

def save_merged_xlsx(merged_sheets, output_filename):
    with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
        for sheet_name, df in merged_sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

def main():
    # Example usage: pass list of input xlsx files with 'apic' prefix
    filenames = [
        'apic_n1_interface_20250917_1552.xlsx', 
        'apic_n2_interface_20250917_1553.xlsx', 
        'apic_p1_interface_20250917_1553.xlsx', 
        'apic_p2_interface_20250917_1554.xlsx'
    ]  # Replace with actual file paths

    apic, datatype = extract_apic_and_datatype(filenames)

    timestamp = time.strftime('%Y%m%d_%H%M')
    output_filename = f'{apic}_merged_{datatype}_{timestamp}.xlsx'
    merged_sheets = merge_xlsx_files(filenames)

    save_merged_xlsx(merged_sheets, output_filename)
    print(f"Merged file saved as: {output_filename}")

if __name__ == "__main__":
    main()
