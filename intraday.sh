#!/bin/bash
export CRT_DIR=${PWD}
cd ${HOME}/c
./stx_ana.exe --intraday
cd ${HOME}/python
python stx_mail.py --intraday
cd ${CRT_DIR}
