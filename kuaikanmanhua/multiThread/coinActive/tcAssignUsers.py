# -*- coding: utf-8 -*-

import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
from setting import *
import MySQLdb
from MySQLdb.cursors import DictCursor

"""
计算kk币领取的用户量
"""

tablePrefix = "activity_assign_"

def coinUseNum(inputTime, outputFile):
    # 数据源链接
    con = MySQLdb.connect(host=kkb_host, port=kkb_port, user=kkb_user, passwd=kkb_passwd, db=kkb_db, charset='utf8', cursorclass=DictCursor)
    cur = con.cursor()
    totalNum = 0
    for i in range(0, 128):
        index = "%03d" % i
        tableName = tablePrefix + index
        sql = "select count(distinct(user_id)) as num from " + tableName
        cur.execute(sql)
        res = cur.fetchone()
        num = res["num"]
        if num != None :
            totalNum = totalNum + num
            print "totalNum : " + str(totalNum) + "; tableNum : " + str(num) + " ; table : " + tableName
        else :
            print "totalNum : " + str(totalNum) + "; tableNum : 0 ; table : " + tableName
    with open(outputFile + "." + str(inputTime) , "w") as outFile:
        outLine = "tcAssignUsers={0}".format(totalNum)
        outFile.write(outLine + "\n")

    cur.close()
    con.close()


if __name__ ==  "__main__":
    inputTime = "2017041410"
    outputFile = "D://data//tcAssignUsers.dat"
    # inputTime = sys.argv[1]
    #outputFile = sys.argv[2]
    coinUseNum(inputTime, outputFile)