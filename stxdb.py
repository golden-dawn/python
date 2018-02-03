from contextlib import closing
import os
import psycopg2
import re
import sys

this = sys.modules[__name__]
this.cnx = None


def db_get_cnx():
    if this.cnx is None:
        this.cnx = psycopg2.connect(os.getenv('DB_URL'))
        this.cnx.autocommit = True
    return this.cnx


# TODO: this does not cleanup properly
def disconnect():
    this.cnx.close()


# read commands perform read only operations: select, describe, show
def db_read_cmd(sql):
    with closing(db_get_cnx().cursor()) as crs:
        crs.execute(sql)
        res = list(crs.fetchall())
    return res


# write commands perform operations that need commit
def db_write_cmd(sql):
    print('db_write_cmd: sql = {0:s}'.format(sql))
    with closing(db_get_cnx().cursor()) as crs:
        # print('sql = {0:s}'.format(sql))
        crs.execute(sql)
        this.cnx.commit()


# Create a database table if it doesn't exist
def db_create_missing_table(tbl_name, sql_create_tbl_cmd):
    res = db_read_cmd("SELECT table_name FROM information_schema.tables "
                      "WHERE table_schema='public' AND table_name='{0:s}'".
                      format(tbl_name))
    if not res:
        db_write_cmd(sql_create_tbl_cmd.format(tbl_name))


def db_get_table_columns(tbl_name):
    res = db_read_cmd("select column_name,udt_name,character_maximum_length,"
                      "numeric_precision,numeric_scale from "
                      "INFORMATION_SCHEMA.COLUMNS where table_name='{0:s}'".
                      format(tbl_name))
    return res


def db_get_key_len(tbl_name):
    res = db_read_cmd(
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type "
        "FROM   pg_index i "
        "JOIN   pg_attribute a ON a.attrelid = i.indrelid "
        "AND a.attnum = ANY(i.indkey) "
        "WHERE  i.indrelid = '{0:s}'::regclass "
        "AND    i.indisprimary".format(tbl_name))
    return len(res)


# Upload data from a file.  Prior to updating, verify that there
# are no duplicate primary keys in the upload file.  If any
# duplicates are found, keep only the first occurrence; discard
# and print error messages for each subsequent occurrence.
def db_upload_file(file_name, tbl_name, sep='\t'):
    with open(file_name, 'r') as f:
        lines = f.readlines()
    db_bulk_upload(tbl_name, lines, sep)


def db_bulk_upload(tbl_name, data, sep='\t'):
    key_len = db_get_key_len(tbl_name)
    tbl_cols = db_get_table_columns(tbl_name)
    p = re.compile('^varchar|date|char')
    use_quotes = [not not p.match(x[1]) for x in tbl_cols]
    cmd = "INSERT INTO '{0:s}' ({1:s}) VALUES ".format(
        tbl_name, ', '.join(["'{0:s}'".format(x[0]) for x in tbl_cols]))
    dct = {}
    write_lines = []
    for line in data:
        if line.strip() == '':
            continue
        tokens = line.strip().split(sep)
        key = '\t'.join(tokens[:key_len])
        if key in dct:
            print('DUPLICATE KEY: {0:s}'.format(key))
            print('  Removed line: {0:s}'.format(line.strip()))
            print('  Prev occurence: {0:s}'.format(dct[key].strip()))
        else:
            dct[key] = line
            new_line = ["'{0:s}'".format(x) if y else x
                        for x, y in zip(tokens, use_quotes)]
            write_lines.append('({0:s})'.format(','.join(new_line)))
    cmd = '{0:s} {1:s}'.format(cmd, ','.join(write_lines))
    db_write_cmd(cmd)


# Return sql string for specific timeframe between start date (sd) and
# end date (ed).  If either start or end date is None this will return
# everything before end date (if sd is None),or everything after start
# date (if ed is None)
def db_sql_timeframe(sd, ed, include_and):
    res = ''
    prefix = ' and' if include_and else ' where'
    if sd is not None and ed is not None:
        res = "{0:s} dt between '{1:s}' and '{2:s}'".format(prefix, sd, ed)
    elif sd is not None:
        res = "{0:s} dt >= '{1:s}'".format(prefix, sd)
    elif ed is not None:
        res = "{0:s} dt <= '{1:s}'".format(prefix, ed)
    return res
