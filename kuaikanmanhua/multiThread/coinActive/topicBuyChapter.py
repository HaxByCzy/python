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
三部作品的购习章节
"""

tablePrefix = "comic_deal_record_"

def coinUseNum(inputTime, outputFile):
    # 数据源链接
    con = MySQLdb.connect(host=kkb_host, port=kkb_port, user=kkb_user, passwd=kkb_passwd, db=auth_db, charset='utf8', cursorclass=DictCursor)
    cur = con.cursor()
    hdsNum = 0
    cqytNum = 0
    bdwzNum = 0
    for i in range(0, 1024):
        index = "%04d" % i
        tableName = tablePrefix + index
        sql = "SELECT parent_id as topic_id , count(*) as num from " + tableName + " where parent_id = " + str(hdsId) + " or parent_id = " + str(cqytId) + " or parent_id = " + str(bdwzId) + " GROUP BY topic_id"
        cur.execute(sql)
        res = cur.fetchall()
        if len(res) > 0 :
            for row in res:
                source = row["topic_id"]
                num = row["num"]
                if source == hdsId:
                    hdsNum = hdsNum + num
                elif source == cqytId:
                    cqytNum = cqytNum + num
                elif source == bdwzId:
                    bdwzNum = bdwzNum + num
            print "hdsNum : " + str(hdsNum) + "; cqytNum : " + str(cqytNum) + "; bdwzNum : " + str(bdwzNum) + " , " + tableName
        else:
            print tableName + "is None"

    with open(outputFile + "." + str(inputTime), "w") as outFile:
        outLine = ",".join(map(str, [hdsNum, cqytNum, bdwzNum]))
        print outLine
        outFile.write(outLine + "\n")
    cur.close()
    con.close()


if __name__ ==  "__main__":
    inputTime = "2017041410"
    outputFile = "D://data//topicBuy.dat"
    # inputTime = sys.argv[1]
    #outputFile = sys.argv[2]
    coinUseNum(inputTime, outputFile)