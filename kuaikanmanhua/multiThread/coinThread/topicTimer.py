# -*- coding: utf-8 -*-

import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
from setting import *
import MySQLdb
from MySQLdb.cursors import DictCursor

#三部作品赠币与总消耗与使用人数

def coinUseNum(inputTime, outputFile):
    # 数据源链接
    con = MySQLdb.connect(host=timer_host, port=timer_port, user=timer_user, passwd=timer_passwd, db=timer_db, charset='utf8', cursorclass=DictCursor)
    cur = con.cursor()
    hdsNum = []
    cqytNum = []
    bdwzNum = []
    #赠币消耗量
    sql = "SELECT topic_id, sum(inverse_num) as used, COUNT(*) as users from comic_order_inverse where (topic_id = " + str(hdsId) + " or topic_id = " + str(bdwzId) + " or topic_id = " + str(cqytId) + ") and pay_type = -1 GROUP BY topic_id;"
    cur.execute(sql)
    res = cur.fetchall()
    if len(res) > 0:
        for row in res:
            topic_id = int(row["topic_id"])
            used = int(row["used"])
            users = int(row["users"])
            if topic_id == hdsId:
                hdsNum.append(used)
                hdsNum.append(users)
            elif topic_id == bdwzId:
                bdwzNum.append(used)
                bdwzNum.append(users)
            elif topic_id == cqytId:
                cqytNum.append(used)
                cqytNum.append(users)
    else:
        hdsNum.append(0)
        cqytNum.append(0)
        bdwzNum.append(0)
        hdsNum.append(0)
        cqytNum.append(0)
        bdwzNum.append(0)
    #购买币消耗量
    sql = "SELECT topic_id, sum(inverse_num) as used, COUNT(*) as users from comic_order_inverse where (topic_id = " + str(hdsId) + " or topic_id = " + str(bdwzId) + " or topic_id = " + str(cqytId) + ")	and pay_type != -1 GROUP BY topic_id;"
    cur.execute(sql)
    res = cur.fetchall()
    if len(res) > 0:
        for row in res:
            topic_id = int(row["topic_id"])
            used = int(row["used"])
            users = int(row["users"])
            if topic_id == hdsId:
                hdsNum.append(used)
                hdsNum.append(users)
            elif topic_id == bdwzId:
                bdwzNum.append(used)
                bdwzNum.append(users)
            elif topic_id == cqytId:
                cqytNum.append(used)
                cqytNum.append(users)
    else:
        hdsNum.append(0)
        cqytNum.append(0)
        bdwzNum.append(0)
        hdsNum.append(0)
        cqytNum.append(0)
        bdwzNum.append(0)

    print hdsNum
    print cqytNum
    print bdwzNum
    with open(outputFile + "." + inputTime, "w") as outFile:
        outLine = "hds," + ",".join(map(str, hdsNum))
        outFile.write(outLine + "\n")
        outLine = "cqyt," + ",".join(map(str, cqytNum))
        outFile.write(outLine + "\n")
        outLine = "bdwz," + ",".join(map(str, bdwzNum))
        outFile.write(outLine + "\n")
    cur.close()
    con.close()


if __name__ ==  "__main__":
    inputTime = "2017041410"
    outputFile = "D://data//topicTimer.dat"
    # inputTime = sys.argv[1]
    # outputFile = sys.argv[2]
    coinUseNum(inputTime, outputFile)