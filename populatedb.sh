#!/bin/bash

python stx_ng.py
python opteod.py
python stx_eod.py --stooq
python stx_eod.py --batch