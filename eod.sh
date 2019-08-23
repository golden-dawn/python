#!/bin/bash
export CRT_DIR=${PWD}
cd ${HOME}/c
./stx_ana.exe --eod
cd ${HOME}/python
python stx_mail.py --eod
cd ${CRT_DIR}
