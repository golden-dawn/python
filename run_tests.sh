#!/bin/bash

echo 'Copy the spots to file'
psql -d stx -c "copy (select * from opt_spots where stk in ('AEOS', 'EXPE', 'NFLX', 'TIE')) to '/tmp/opt_spots.txt'"
psql -d stx -c "copy (select * from opt_spots where stk in ('AA', 'VXX')) to '/tmp/opt_spots_ed.txt'"

if [ -z "$1" ]
then
    echo "No test database name supplied, using default (stx_test)"
    DB_NAME=stx_test
else
    DB_NAME=$1
fi
echo 'Removing the existing databases'
dropdb $DB_NAME
echo 'Creating the main and test databases'
createdb $DB_NAME
echo 'Dumping the main database schema,and copying into the test database'
pg_dump --schema-only -f /tmp/stx.sql stx
psql $DB_NAME < /tmp/stx.sql
echo 'adding function that duplicates a table, including indexes & foreign keys'
psql $DB_NAME < create_table_like.sql
echo 'pointing DB_URL to the test database'
export DB_URL0=$DB_URL
export DB_URL="${DB_URL/stx/$DB_NAME}"
echo 'Running the tests'
nosetests stxtest.py
echo 'resetting DB_URL to the main database'
export DB_URL=$DB_URL0
echo 'All done'
