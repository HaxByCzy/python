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
计算kk币总使用量
"""

tablePrefix = "activity_consume_"

def coinUseNum(inputTime, outputFile):
    # 数据源链接
    con = MySQLdb.connect(host=kkb_host, port=kkb_port, user=kkb_user, passwd=kkb_passwd, db=kkb_db, charset='utf8', cursorclass=DictCursor)
    cur = con.cursor()
    userNum = 0
    feeNum = 0
    for i in range(0, 128):
        index = "%03d" % i
        tableName = tablePrefix + index
        sql = "select count(distinct(user_id)) as users, sum(consume_fee) as fee from " + tableName
        cur.execute(sql)
        res = cur.fetchone()
        fee = res["fee"]
        users = int(res["users"])
        if fee != None:
            feeNum = feeNum + fee
        if users > 0:
            userNum = userNum + users
        print userNum, feeNum, users, fee, tableName

    with open(outputFile + "." + str(inputTime) , "w") as outFile:
        outLine = "userNum={0},feeNum={1}".format(userNum, feeNum)
        outFile.write(outLine + "\n")

    cur.close()
    con.close()


if __name__ ==  "__main__":
    inputTime = "2017041410"
    outputFile = "D://data//tcConsume.dat"
    # inputTime = sys.argv[1]
    # outputFile = sys.argv[2]
    coinUseNum(inputTime, outputFile)