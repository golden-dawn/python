#!/bin/bash
export CRT_DIR=${PWD}
cd ${HOME}
source ${HOME}/.env_vars
source ${HOME}/.bash_profile
source ${HOME}/.bash_profile.pysave
cd ${HOME}/c
./stx_ana_1.exe --intraday --cron
cd ${HOME}/python
source .env
export LANG=en_US.UTF-8
python3 stx_247.py -i -c -s 15 
cd ${CRT_DIR}
