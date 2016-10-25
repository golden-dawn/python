import pymysql

def connect():
    #cnx = pymysql.connect(host='127.0.0.1', user='root', database='goldendawn')
    cnx = pymysql.connect(host='127.0.0.1', user='root',
                          password='m1y2s3q7l8', database='goldendawn')
    return cnx

def disconnect(cnx):
    cnx.close()
