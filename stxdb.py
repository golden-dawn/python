import pymysql
import sys

class StxDB :
    
    cnx = None
    
    @staticmethod
    def get_connection() :
        if StxDB.cnx is None :
            StxDB.cnx = pymysql.connect(host='127.0.0.1',
                                        user='root',
                                        password='m1y2s3q7l8',
                                        database='goldendawn')
            print('Connected to goldendawn database')

    # TODO: this does not cleanup properly
    @staticmethod
    def disconnect() :
        StxDB.cnx.close()

    # read commands perform read only operations: select, describe, show
    @staticmethod
    def read_cmd(sql) :
        StxDB.get_connection()
        with StxDB.cnx as crs :
            crs.execute(sql)
        return crs.fetchall()

    # write commands perform operations that need commit
    @staticmethod
    def write_cmd(sql) :
        StxDB.get_connection()
        with StxDB.cnx as crs :
            crs.execute(sql)
            StxDB.cnx.commit()

    # Create a database table if it doesn't exist
    @staticmethod
    def create_missing_table(tbl_name, sql_create_tbl_cmd) :
        res = StxDB.read_cmd("show tables like '{0:s}'".format(tbl_name))
        if len(res) == 0 :
            StxDB.write_cmd(sql_create_tbl_cmd.format(tbl_name))

    @staticmethod
    def get_key_len(tbl_name) :
        res      = StxDB.read_cmd('describe {0:s}'.format(tbl_name))
        tbl_keys = [x for x in res if x[3] == 'PRI']
        return len(tbl_keys)
            
    # Upload data from a file.  Prior to updating, verify that there
    # are no duplicate primary keys in the upload file.  If any
    # duplicates are found, keep only the first occurrence; discard
    # and print error messages for each subsequent occurrence.    
    @staticmethod
    def upload_file(file_name, tbl_name, key_len) :
        dct                  = {}
        with open(file_name, 'r+') as f :
            lines            = f.readlines()
            write_lines      = []
            for line in lines :
                tokens       = line.split()
                key          = '\t'.join(tokens[:key_len])
                if key in dct :
                    print('DUPLICATE KEY: {0:s}'.format(key))
                    print('  Removed line: {0:s}'.format(line.strip()))
                    print('  Prev occurence: {0:s}'.format(dct[key].strip()))
                else :
                    dct[key] = line
                    write_lines.append(line.strip())
            f.seek(0)
            f.write('\n'.join(write_lines))
            f.truncate()                    
        StxDB.write_cmd("load data infile '{0:s}' into table {1:s}". \
                        format(file_name, tbl_name))

        
def connect():
    #cnx = pymysql.connect(host='127.0.0.1', user='root', database='goldendawn')
    print('Entered stxdb.connect()')
    cnx = pymysql.connect(host='127.0.0.1', user='root',
                          password='m1y2s3q7l8', database='goldendawn')
    return cnx

def disconnect(cnx):
    cnx.close()
