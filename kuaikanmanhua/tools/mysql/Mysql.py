# -*- coding: utf-8 -*-

import sys
defaultencoding = 'utf-8'
from setting import *
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
import MySQLdb
from MySQLdb.cursors import DictCursor

class Mysql(object):
    """
    mysql数据库数据操作工具类
    """
    def __init__(self, host, port, db, username, pwd):
        self.con = MySQLdb.connect(host=host, port=port, user=username, passwd=pwd, db=db, charset='utf8', cursorclass=DictCursor)
        self.cur = self.con.cursor()

    def loadFile(self, sql):
        """
        根据sql语句，将文件内容加入到数据库中
        :param sql:
        :return:
        """
        print sql
        try:
            self.cur.execute(sql)
            self.con.commit()
        except Exception as e:
            print e

    def insert(self, sql):
        try:
            self.cur.execute(sql)
            self.con.commit()
        except Exception as e:
            print e

    def __del__(self):
        self.cur.close()
        self.con.close()

if __name__ == "__main__":
    mysql = Mysql(host, port, db, username, pwd)
    #sql = "load data local infile 'D://data//app.dat' into table all_app fields terminated by '\t' (app, num, created_at)"
    sql = sys.argv[1].strip()
    mysql.loadFile(sql)
