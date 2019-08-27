#!/bin/bash
export CRT_DIR=${PWD}
cd ${HOME}
source .env
source .bash_profile
source .bash_profile.pysave
cd ${HOME}/c
./stx_ana.exe -eod
cd ${HOME}/python
source .env
python stx_mail.py --eod
cd ${CRT_DIR}
