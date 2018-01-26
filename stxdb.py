import sqlite3
import sys

this = sys.modules[__name__]

this.cnx = None
this.sqlite_file = '/home/cma/stx.sqlite'


def db_get_cnx():
    if this.cnx is None:
        this.cnx = sqlite3.connect(this.sqlite_file)
    return this.cnx


# TODO: this does not cleanup properly
def disconnect():
    this.cnx.close()


# read commands perform read only operations: select, describe, show
def db_read_cmd(sql):
    with db_get_cnx() as crs:
        crs.execute(sql)
    return crs.fetchall()


# write commands perform operations that need commit
def db_write_cmd(sql):
    with db_get_cnx() as crs:
        crs.execute(sql)
        this.cnx.commit()


# Create a database table if it doesn't exist
def db_create_missing_table(tbl_name, sql_create_tbl_cmd):
    res = db_read_cmd("show tables like '{0:s}'".format(tbl_name))
    if len(res) == 0:
        db_write_cmd(sql_create_tbl_cmd.format(tbl_name))


def db_get_key_len(tbl_name):
    res = db_read_cmd('describe {0:s}'.format(tbl_name))
    tbl_keys = [x for x in res if x[3] == 'PRI']
    return len(tbl_keys)


# Upload data from a file.  Prior to updating, verify that there
# are no duplicate primary keys in the upload file.  If any
# duplicates are found, keep only the first occurrence; discard
# and print error messages for each subsequent occurrence.
def db_upload_file(file_name, tbl_name, key_len):
    dct = {}
    with open(file_name, 'r+') as f:
        lines = f.readlines()
        write_lines = []
        for line in lines:
            tokens = line.split()
            key = '\t'.join(tokens[:key_len])
            if key in dct:
                print('DUPLICATE KEY: {0:s}'.format(key))
                print('  Removed line: {0:s}'.format(line.strip()))
                print('  Prev occurence: {0:s}'.format(dct[key].strip()))
            else:
                dct[key] = line
                write_lines.append(line.strip())
        f.seek(0)
        f.write('\n'.join(write_lines))
        f.truncate()
        db_write_cmd("load data infile '{0:s}' into table {1:s}".
                     format(file_name, tbl_name))


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
