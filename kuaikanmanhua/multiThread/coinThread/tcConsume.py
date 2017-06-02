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

#计算kk币使用

lock = threading.Lock()
tablePrefix = "activity_consume_"
userNum = 0
feeNum = 0

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
        threadUser = 0
        threadFee = 0
        for i in range(self.beginIndex, self.endIndex + 1):
            index = "%03d" % i
            tableName = tablePrefix + index
            sql = "select count(distinct(user_id)) as users, sum(consume_fee) as fee from " + tableName
            self.cur.execute(sql)
            res = self.cur.fetchone()
            fee = res["fee"]
            users = int(res["users"])
            if fee != None:
                threadFee = threadFee + fee
            if users > 0:
                threadUser = threadUser + users
            print self.threadName,threadUser, threadFee, users, fee
        self.cur.close()
        self.con.close()
        self.changeGlobal(threadUser, threadFee)


    def changeGlobal(self, user, fee):
        global userNum
        global feeNum
        lock.acquire()
        try:
            userNum = userNum + user
            feeNum = feeNum + fee
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
        outLine = "userNum={0},feeNum={1}".format(userNum, feeNum)
        outFile.write(outLine + "\n")


if __name__ ==  "__main__":
    inputTime = "2017041410"
    outputFile = "D://data//tcConsume.dat"
    # inputTime = sys.argv[1]
    # outputFile = sys.argv[2]
    jobStart(inputTime, outputFile)