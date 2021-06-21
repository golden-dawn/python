#!/bin/bash
export CRT_DIR=${PWD}
cd ${HOME}
source ${HOME}/.env_vars
cd ${HOME}/c
./stx_ana_1.exe --intraday-expiry
cd ${CRT_DIR}
