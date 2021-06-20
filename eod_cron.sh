#!/bin/bash
export CRT_DIR=${PWD}
cd ${HOME}
source ${HOME}/.env_vars
cd ${HOME}/c
./stx_ana_1.exe --eod --cron
cd ${HOME}/python
source ${HOME}/.envs/stx/bin/activate
export LANG=en_US.UTF-8
python3 stx_247.py -e -c -s 15
cd ${CRT_DIR}
