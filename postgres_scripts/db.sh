#!/bin/bash

DB_NAME=stx_ng
if [ -z "$1" ]
then
    TEST=''
else
    TEST=test
fi
echo 'Removing the eod database, if existent'
dropdb $DB_NAME
echo 'Creating the eod database'
createdb $DB_NAME
echo 'Dumping the main database schema,and copying into the test database'
pg_dump --schema-only -f /tmp/stx.sql stx
sed -i -e 's/varying(8)/varying(16)/g' /tmp/stx.sql
psql -q $DB_NAME < /tmp/stx.sql
echo 'adding function that duplicates a table, including indexes & foreign keys'
psql -q $DB_NAME < create_table_like.sql
echo 'pointing DB_URL to current database'
export DB_URL0=$DB_URL
export DB_URL="${DB_URL/stx/$DB_NAME}"

echo 'Populating the EOD database'
python stx_norgate.py $TEST

echo 'resetting DB_URL to the main database'
export DB_URL=$DB_URL0
echo 'All done'
