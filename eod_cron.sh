#!/bin/bash
export CRT_DIR=${PWD}
cd ${HOME}
source .env
source .bash_profile
source .bash_profile.pysave
cd ${HOME}/c
./stx_ana_1.exe --eod --cron
cd ${HOME}/python
source .env
export LANG=en_US.UTF-8
python3 stx_247.py -e -c -m -s 15
cd ${CRT_DIR}
