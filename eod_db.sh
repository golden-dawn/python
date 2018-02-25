#!/bin/bash

echo 'Copy the spots to file'
psql -d stx -c "copy (select * from opt_spots) to '/tmp/opt_spots.txt'"

if [ -z "$1" ]
then
    echo "No test database name supplied, using default (stx_1)"
    DB_NAME=stx_1
else
    DB_NAME=$1
fi
echo 'Creating the eod database'
createdb $DB_NAME
echo 'Dumping the main database schema,and copying into the test database'
pg_dump --schema-only -f /tmp/stx.sql stx
psql $DB_NAME < /tmp/stx.sql
echo 'adding function that duplicates a table, including indexes & foreign keys'
psql $DB_NAME < create_table_like.sql
echo 'pointing DB_URL to the test database'
export DB_URL0=$DB_URL
export DB_URL="${DB_URL/stx/$DB_NAME}"
echo 'Populating the EOD database'
python stxeod.py
echo 'Applying adjustments'
./adjustments/20010102_20121231.sh
./adjustments/20130102_20131115.sh
./adjustments/20131118_20160823.sh
echo 'resetting DB_URL to the main database'
export DB_URL=$DB_URL0
echo 'All done'
