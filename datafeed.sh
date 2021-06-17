#!/bin/bash                                                                     
export CRT_DIR=${PWD}
cd ${HOME}
source ${HOME}/.env_vars
cd ${HOME}/python
source ${HOME}/.envs/stx/bin/activate
export LANG=en_US.UTF-8
python3 stx_datafeed.py
cd ${CRT_DIR}
