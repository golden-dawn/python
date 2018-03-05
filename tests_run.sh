#!/bin/bash

if [ -z "$1" ]
then
    echo "No test database name supplied, using default (test_stx)"
    DB_NAME=test_stx
else
    DB_NAME=$1
fi
export TEST_STX="('AEOS', 'EXPE', 'NFLX', 'TIE', 'AA', 'VXX', 'KO', 'MCT')"
echo 'Removing the existing databases'
dropdb $DB_NAME
echo 'Creating the test database'
createdb $DB_NAME
echo 'Dumping the main database schema, and copying into the test database'
pg_dump --schema-only stx_1 | psql $DB_NAME
echo 'adding function that duplicates a table, including indexes & foreign keys'
psql $DB_NAME < create_table_like.sql

echo 'Copy the exchanges table from stx'
pg_dump -a -t exchanges stx | psql -q $DB_NAME

echo 'Copy the equity info for the test stocks to test database'
psql -d stx_1 -c "copy (select * from equities where ticker in $TEST_STX) to stdout" | psql -d $DB_NAME -c "copy equities from stdin"

echo 'Copy the spots for the test stocks to test database'
psql -d stx_1 -c "copy (select * from opt_spots where stk in $TEST_STX) to stdout" | psql -d $DB_NAME -c "copy opt_spots from stdin"

echo 'Copy the deltaneutral dividends for the test stocks to test database'
psql -d stx_1 -c "copy (select * from dn_dividends where stk in $TEST_STX) to stdout" | psql -d $DB_NAME -c "copy dn_dividends from stdin"
echo 'Copy the deltaneutral eods for the test stocks to test database'
psql -d stx_1 -c "copy (select * from dn_eods where stk in $TEST_STX) to stdout" | psql -d $DB_NAME -c "copy dn_eods from stdin"

echo 'Copy the eoddata dividends for the test stocks to test database'
psql -d stx_1 -c "copy (select * from ed_dividends where stk in $TEST_STX) to stdout" | psql -d $DB_NAME -c "copy ed_dividends from stdin"
echo 'Copy the eoddata eods for the test stocks to test database'
psql -d stx_1 -c "copy (select * from ed_eods where stk in $TEST_STX) to stdout" | psql -d $DB_NAME -c "copy ed_eods from stdin"

echo 'Copy the marketdata dividends for the test stocks to test database'
psql -d stx_1 -c "copy (select * from md_dividends where stk in $TEST_STX) to stdout" | psql -d $DB_NAME -c "copy md_dividends from stdin"
echo 'Copy the marketdata eods for the test stocks to test database'
psql -d stx_1 -c "copy (select * from md_eods where stk in $TEST_STX) to stdout" | psql -d $DB_NAME -c "copy md_eods from stdin"

echo 'Copy the dividends for the test stocks to test database'
psql -d stx_1 -c "copy (select * from my_dividends where stk in $TEST_STX) to stdout" | psql -d $DB_NAME -c "copy my_dividends from stdin"
echo 'Copy the eods for the test stocks to test database'
psql -d stx_1 -c "copy (select * from my_eods where stk in $TEST_STX) to stdout" | psql -d $DB_NAME -c "copy my_eods from stdin"

echo 'Copy the stooq dividends for the test stocks to test database'
psql -d stx_1 -c "copy (select * from sq_dividends where stk in $TEST_STX) to stdout" | psql -d $DB_NAME -c "copy sq_dividends from stdin"
echo 'Copy the stooq eods for the test stocks to test database'
psql -d stx_1 -c "copy (select * from sq_eods where stk in $TEST_STX) to stdout" | psql -d $DB_NAME -c "copy sq_eods from stdin"

echo 'pointing DB_URL to the test database'
export DB_URL0=$DB_URL
export DB_URL="${DB_URL/stx/$DB_NAME}"
echo 'Running the tests'
nosetests recon_test.py
echo 'resetting DB_URL to the main database'
export DB_URL=$DB_URL0
echo 'All done'
