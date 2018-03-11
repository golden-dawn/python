#!/bin/bash

if [ -z "$1" ]
then
    echo "No test database name supplied, using default (stx_ng)"
    DB_NAME=stx_ng
else
    DB_NAME=$1
fi
echo 'Creating the eod database'
createdb $DB_NAME
echo 'Dumping the main database schema,and copying into the test database'
pg_dump --schema-only stx | psql -q $DB_NAME
echo 'adding function that duplicates a table, including indexes & foreign keys'
psql -q $DB_NAME < create_table_like.sql
echo 'pointing DB_URL to current database'
export DB_URL0=$DB_URL
export DB_URL="${DB_URL/stx/$DB_NAME}"

echo 'Populating the EOD database'
python stx_norgate.py

echo 'resetting DB_URL to the main database'
export DB_URL=$DB_URL0
echo 'All done'
