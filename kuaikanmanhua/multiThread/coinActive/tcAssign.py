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
计算不同页面kk币领取量
"""

tablePrefix = "activity_assign_"

def coinUseNum(inputTime, outputFile):
    # 数据源链接
    con = MySQLdb.connect(host=kkb_host, port=kkb_port, user=kkb_user, passwd=kkb_passwd, db=kkb_db, charset='utf8', cursorclass=DictCursor)
    cur = con.cursor()
    buyNum = 0
    activeNum = 0
    giveNum = 0
    for i in range(0, 128):
        index = "%03d" % i
        tableName = tablePrefix + index
        sql = "select source_type as source, sum(assign_fee) as fee from " + tableName + " GROUP BY source"
        cur.execute(sql)
        res = cur.fetchall()
        if len(res) > 0 :
            for row in res:
                source = int(row["source"])
                fee = int(row["fee"])
                if source == 1:
                    buyNum = buyNum + fee
                elif source == 2:
                    activeNum = activeNum + fee
                elif source == 3:
                    giveNum = giveNum + fee
            print buyNum, activeNum, giveNum, tableName
        else:
            print tableName + "is None"

    totalNum = buyNum + activeNum + giveNum
    with open(outputFile + "." + str(inputTime), "w") as outFile:
        outLine = ",".join(map(str, [totalNum, activeNum, buyNum, giveNum]))
        outFile.write(outLine + "\n")
    cur.close()
    con.close()


if __name__ ==  "__main__":
    inputTime = "2017041410"
    outputFile = "D://data//tcAssign.dat"
    # inputTime = sys.argv[1]
    # outputFile = sys.argv[2]
    coinUseNum(inputTime, outputFile)