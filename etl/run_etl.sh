#!/bin/bash

echo "Start Luigi ETL Pipeline Process"

# # Virtual Environment Path
# VENV_PATH="./venv/bin/activate"

# # Activate venv
# source "$VENV_PATH"

# set python script
PYTHON_SCRIPT="./etl_process.py"

# run python script
python "$PYTHON_SCRIPT" >> ./log/logfile.log 2>&1

# logging simple
dt=$(date '+%d/%m/%Y %H:%M:%S');
echo "Luigi Started at ${dt}" >> ./log/luigi-info.log

echo "End Luigi ETL Pipeline Process"