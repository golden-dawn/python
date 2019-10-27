#!/bin/bash
export CRT_DIR=$(pwd)
export VOLUME_NAME=${1}
export LAST_DATE=${2}
export EXP1=${3}
export EXP2=${4}
export EXP3=${5}
export EXP4=${6}
sed -e "s/LAST_DT/${LAST_DATE}/g" -e "s/EXP1/${EXP1}/g" -e "s/EXP2/${EXP2}/g" -e "s/EXP3/${EXP3}/g" -e "s/EXP4/${EXP4}/g" backup_db.sql > /tmp/backup_db.sql
echo "psql -d stx -f /tmp/backup_db.sql"
psql -d stx -f /tmp/backup_db.sql
cd /tmp
tar cvf db_bkp.tar eods.csv divis.csv ldrs.csv setups.csv opts.csv
gzip db_bkp.tar
cp db_bkp.tar.gz /Volumes/${VOLUME_NAME}/