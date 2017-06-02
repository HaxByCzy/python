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

#计算kk币领取的用户量

lock = threading.Lock()
tablePrefix = "activity_assign_"
totalNum = 0

class TheadWorker(threading.Thread):
    def __init__(self, threadName, beginIndex, endIndex):
        threading.Thread.__init__(self)
        self.threadName = threadName
        self.beginIndex = beginIndex
        self.endIndex = endIndex
        # db connect
        self.con = MySQLdb.connect(host=kkb_host, port=kkb_port, user=kkb_user, passwd=kkb_passwd, db=kkb_db, charset='utf8', cursorclass=DictCursor)
        self.cur = self.con.cursor()

    def run(self):
        threadTotal = 0
        for i in range(self.beginIndex, self.endIndex + 1):
            index = "%03d" % i
            tableName = tablePrefix + index
            sql = "select count(distinct(user_id)) as num from " + tableName
            self.cur.execute(sql)
            res = self.cur.fetchone()
            num = res["num"]
            if num != None :
                threadTotal = threadTotal + num
                print self.threadName, threadTotal, num, tableName
            else :
                print self.threadName,threadTotal,"-0-", tableName
        self.cur.close()
        self.con.close()
        self.changeGlobal(threadTotal)


    def changeGlobal(self, total):
        global totalNum
        lock.acquire()
        try:
            totalNum = totalNum + total
        finally:
            lock.release()

def jobStart(inputTime, outputFile):

    threadNum = 4
    baseNum = int(127 / threadNum)
    beginIndex = 0
    endIndex = 0
    twList = []
    for i in range(0, threadNum):
        beginIndex =  beginIndex
        endIndex = beginIndex + baseNum
        if endIndex > 127 :
            endIndex = 127
        tw = TheadWorker("thread" + str(i) ,beginIndex ,endIndex)
        twList.append(tw)
        print "thread" + str(i), beginIndex, endIndex
        beginIndex = endIndex + 1
    for tw in twList:
        tw.start()
    for tw in twList:
        tw.join()
    with open(outputFile + "." + str(inputTime) , "w") as outFile:
        outLine = "tcAssignUsers={0}".format(totalNum)
        outFile.write(outLine + "\n")


if __name__ ==  "__main__":
    inputTime = "2017041410"
    outputFile = "D://data//tcAssignUsers.dat"
    # inputTime = sys.argv[1]
    #outputFile = sys.argv[2]
    jobStart(inputTime, outputFile)