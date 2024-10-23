# py_aciscript

## Project Structure

```sh
py_aciscript
└── config
   ├── logging.dev.json       <- log config
   ├── logging.prod.json
   └── n1_apic_input.json     <- input config file
└── log                       <- log directory
└── src
   ├── pyapicapi.py           <- module for apic api
   └── pyapicanaylsis.py      <- module for apic anaylsis
```

## pyapicapi.py

Get cisco apic info using rest api

```sh
usage: pyapicapi.py [-h] [-i INFILES] [-a]

options:
  -h, --help            show this help message and exit
  -i INFILES, --infiles INFILES
                        input json in config folder, example: -i n1_input.json,n2_input.json,n3_input.json
  -a, --anaylsis        flag to analysis and parse table to new excel
```

1. Prepare xxx_input.json config file in config folder
2. Option1: by prompt user to select file
   ````sh
   python src\pyapicapi.py
   ```sh
   ````
3. Option2: by input args
   ```
   python src\pyapicapi.py -i n1_apic_input.json,n2_apic_input.json
   ```

## pyapicanaylsis_contract.py, pyapicanaylsis_interface.py

Anaylsis table from pyapicapi.py

```sh
usage: pyapicanaylsis_interface.py [-h] [-i INFILES]

options:
  -h, --help            show this help message and exit
  -i INFILES, --infiles INFILES
                        input excel from pyapicapi.py, example: -i n1_20240101.xlsx
```
