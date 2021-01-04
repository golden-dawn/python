#!/bin/bash                                                                     
export CRT_DIR=${PWD}
cd ${HOME}
source .env
source .bash_profile
source .bash_profile.pysave
cd ${HOME}/python
source .env
export LANG=en_US.UTF-8
python3 stx_datafeed.py
cd ${CRT_DIR}
