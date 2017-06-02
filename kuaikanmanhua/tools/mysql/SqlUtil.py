# -*- coding: utf-8 -*-

import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
from Mysql import Mysql
from setting import *


def sqlOperate(funName, sql):
    mysql = Mysql(host, port, db, username, pwd)
    print sql
    if funName.strip() == "loadFile":
        mysql.loadFile(sql)
    elif funName.strip() == "insert":
        mysql.insert(sql)
    else :
        print "no mysql method : " + str(funName)


if __name__ == "__main__":
    funName = "insert"
    sql = "insert into up_os (did, uid, day,created_at) values('23515151', '13875336', 5741416, '2017-04-23')"
    sqlOperate(funName, sql)