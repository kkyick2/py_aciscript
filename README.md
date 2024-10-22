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

1. Prepare xxx_input.json config file in config folder
2. Option1: by prompt user to select file
   ```
   python src\pyapicapi.py
   ```
3. Option2: by input args
   ```
   python src\pyapicapi.py -i n1_apic_input.json,n2_apic_input.json
   ```

## pyapicanaylsis_contract.py

Anaylsis table from pyapicapi.py

- epgbd_ip
- filter
- contract
- contract_epgip
