#!/bin/bash
export CRT_DIR=$(pwd)
echo "psql -d stx -f backup_db.sql -v last_dt=$2 -v exp1=$3 -v exp2=$4 -v exp3=$5 -v exp4=$6"
psql -d stx -f backup_db.sql -v last_dt=$2 -v exp1=$3 -v exp2=$4 -v exp3=$5 -v exp4=$6
cd /tmp
tar cvf db_bkp.tar eods.csv divis.csv ldrs.csv setups.csv opts.csv
gzip db_bkp.tar
cp db_bkp.tar.gz /Volumes/$1/