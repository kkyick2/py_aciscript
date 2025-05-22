@echo off
call "C:\Users\%USERNAME%\projects_local\python\py_aciscript\venv\Scripts\activate.bat"
python src\pyapicapi.py -i n1_apic.json,n2_apic.json,p1_apic.json,p2_apic.json -a
