# -*- coding: utf-8 -*-

import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
from setting import *
import MySQLdb
from MySQLdb.cursors import DictCursor
import threading

"""
三部作品的购习章节
"""

lock = threading.Lock()
tablePrefix = "comic_deal_record_"

hdsNum = 0
cqytNum = 0
bdwzNum = 0

class TheadWorker(threading.Thread):
    def __init__(self, threadName, beginIndex, endIndex):
        threading.Thread.__init__(self)
        self.threadName = threadName
        self.beginIndex = beginIndex
        self.endIndex = endIndex
        # db connect
        self.con = MySQLdb.connect(host=kkb_host, port=kkb_port, user=kkb_user, passwd=kkb_passwd, db=auth_db, charset='utf8', cursorclass=DictCursor)
        self.cur = self.con.cursor()

    def run(self):
        hdsThread = 0
        cqytThread = 0
        bdwzThread = 0
        for i in range(self.beginIndex, self.endIndex + 1):
            index = "%04d" % i
            tableName = tablePrefix + index
            sql = "SELECT parent_id as topic_id , count(*) as num from " + tableName + " where parent_id = " + str(hdsId) + " or parent_id = " + str(cqytId) + " or parent_id = " + str(bdwzId) + " GROUP BY topic_id"
            self.cur.execute(sql)
            res = self.cur.fetchall()
            if len(res) > 0 :
                for row in res:
                    source = row["topic_id"]
                    num = row["num"]
                    if source == hdsId:
                        hdsThread += num
                    elif source == cqytId:
                        cqytThread += num
                    elif source == bdwzId:
                        bdwzThread += num
                print
            else:
                print tableName + " is None"

        self.cur.close()
        self.con.close()
        self.changeGlobal(hdsThread, cqytThread, bdwzThread)


    def changeGlobal(self, hdsThread, cqytThread, bdwzThread):
        global hdsNum
        global cqytNum
        global bdwzNum
        lock.acquire()
        try:
            hdsNum += hdsThread
            cqytNum += cqytThread
            bdwzNum += bdwzThread
        finally:
            lock.release()

def jobStart(inputTime, outputFile):

    threadNum = 8
    baseNum = int(1023 / threadNum)
    beginIndex = 0
    endIndex = 0
    twList = []
    for i in range(0, threadNum):
        beginIndex =  beginIndex
        endIndex = beginIndex + baseNum
        if endIndex > 1023 :
            endIndex = 1023
        tw = TheadWorker("thread" + str(i) ,beginIndex ,endIndex)
        twList.append(tw)
        print "thread" + str(i), beginIndex, endIndex
        beginIndex = endIndex + 1
    for tw in twList:
        tw.start()
    for tw in twList:
        tw.join()
    with open(outputFile + "." + str(inputTime), "w") as outFile:
        outLine = ",".join(map(str, [hdsNum, cqytNum, bdwzNum]))
        outFile.write(outLine + "\n")

if __name__ ==  "__main__":
    inputTime = "2017041410"
    outputFile = "D://data//topicBuy.dat"
    # inputTime = sys.argv[1]
    #outputFile = sys.argv[2]
    jobStart(inputTime, outputFile)