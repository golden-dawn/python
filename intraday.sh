#!/bin/bash
export CRT_DIR=${PWD}
cd ${HOME}
source .env
source .bash_profile
source .bash_profile.pysave
cd ${HOME}/c
./stx_ana.exe -intraday
cd ${HOME}/python
source .env
python stx_mail.py --intraday
cd ${CRT_DIR}
