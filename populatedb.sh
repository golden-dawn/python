#!/bin/bash

echo -e "\n\n python stx_ng.py\n\n "
python stx_ng.py
echo -e "\n\n python opteod.py\n\n "
python opteod.py
echo -e "\n\n python stx_eod.py --stooq\n\n "
python stx_eod.py --stooq
echo -e "\n\n python stx_eod.py --batch\n\n "
python stx_eod.py --batch
