#!/bin/bash

if [ -z "$1" ]
then
    echo "No test database name supplied, using default (stx)"
    DB_NAME=stx
else
    DB_NAME=$1
fi

psql -d $DB_NAME -c "insert into dividends values ('ADVS', '2013-07-09', 0.7446, 2)"
psql -d $DB_NAME -c "insert into dividends values ('ALVR', '2013-04-01', 10, 0)"
psql -d $DB_NAME -c "insert into dividends values ('ANTH', '2013-07-12', 7.3333, 0)"
psql -d $DB_NAME -c "insert into dividends values ('ASTM', '2013-10-15', 20, 0)"
psql -d $DB_NAME -c "insert into dividends values ('CBRX', '2013-08-08', 8.1944, 0)"
psql -d $DB_NAME -c "update dividends set ratio=0.8371 where stk='CIG' and date='2013-05-01'"
psql -d $DB_NAME -c "insert into dividends values ('CXW', '2013-04-16', 0.8373, 2)"
psql -d $DB_NAME -c "insert into dividends values ('FBN', '2013-05-28', 6.8687, 0)"
psql -d $DB_NAME -c "insert into dividends values ('FBNI', '2013-05-28', 6.8687, 0)"
psql -d $DB_NAME -c "insert into dividends values ('FBNIQ', '2013-05-28', 6.8687, 0)"
psql -d $DB_NAME -c "insert into dividends values ('GASX', '2013-08-19', 4, 0)"
psql -d $DB_NAME -c "insert into dividends values ('HDY', '2013-06-28', 7.6170, 0)"
psql -d $DB_NAME -c "insert into dividends values ('KCG', '2013-07-03', 3.4489, 0)"
psql -d $DB_NAME -c "insert into dividends values ('NRGY', '2013-06-18', 0.6, 0)"
psql -d $DB_NAME -c "insert into dividends values ('NWS', '2013-06-28', 0.4717, 2)"
psql -d $DB_NAME -c "insert into dividends values ('NWSA', '2013-06-28', 0.4785, 2)"
psql -d $DB_NAME -c "insert into dividends values ('OTEL', '2013-05-24', 5.2301, 0)"
psql -d $DB_NAME -c "insert into dividends values ('PULS', '2013-05-21', 10, 0)"
psql -d $DB_NAME -c "insert into dividends values ('SYNM', '2013-04-11', 10, 0)"
psql -d $DB_NAME -c "insert into dividends values ('USU', '2013-07-01', 25, 0)"
psql -d $DB_NAME -c "delete from eods where stk='WEX' and date<'2013-04-16'"
psql -d $DB_NAME -c "insert into dividends values ('ZLCS', '2013-10-02', 5, 0)"
psql -d $DB_NAME -c "update dividends set ratio=1.3158 where stk='UNTD' and date='2013-10-31'"
