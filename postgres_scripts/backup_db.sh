#!/bin/bash

if [ -z "$1" ]
then
    echo "No database name supplied, exiting ..."
    exit 0
else
    DB_NAME=$1
fi

if [ -z "$DB_BACKUP_DIR" ]
then
    echo "Prior to running this script, define the DB_BACKUP_DIR variable"
    echo "No database backup directory defined, exiting ..."
    exit 0
else
    echo -e 'Creating backup directory for database'
    mkdir -p $DB_BACKUP_DIR/$DB_NAME
fi

pg_dump -Fc $DB_NAME | split -b 1000m - $DB_BACKUP_DIR/$DB_NAME/$DB_NAME
