import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
from setting import *
import MySQLdb
from MySQLdb.cursors import DictCursor
import threading


lock = threading.Lock()
tablePrefix = "activity_assign_"
buyNum = 0
activeNum = 0
giveNum = 0
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
        threadBuy = 0
        threadActive = 0
        threadGive = 0
        threadTotal = 0
        for i in range(self.beginIndex, self.endIndex + 1):
            index = "%03d" % i
            tableName = tablePrefix + index
            sql = "select source_type as source, sum(assign_fee) as fee from " + tableName + " GROUP BY source"
            self.cur.execute(sql)
            res = self.cur.fetchall()
            if len(res) > 0 :
                for row in res:
                    source = int(row["source"])
                    fee = int(row["fee"])
                    if source == 1:
                        threadBuy = threadBuy + fee
                    elif source == 2:
                        threadActive = threadActive + fee
                    elif source == 3:
                        threadGive = threadGive + fee
                print self.threadName, threadBuy, threadActive, threadGive, tableName
            else:
                print self.threadName + " " + tableName + " is None "
        self.cur.close()
        self.con.close()
        threadTotal = threadBuy + threadActive + threadGive
        self.changeGlobal(threadTotal, threadBuy, threadActive, threadGive)


    def changeGlobal(self, total, buy, active, give):
        global buyNum
        global activeNum
        global giveNum
        global totalNum
        lock.acquire()
        try:
            buyNum = buyNum + buy
            activeNum = activeNum + active
            giveNum = giveNum + give
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
    with open(outputFile + "." + str(inputTime), "w") as outFile:
        outLine = ",".join(map(str, [totalNum, activeNum, buyNum, giveNum]))
        outFile.write(outLine + "\n")

if __name__ == "__main__":
    inputTime = "2017041410"
    outputFile = "D://data//tcAssign.dat"
    # inputTime = sys.argv[1]
    # outputFile = sys.argv[2]
    jobStart(inputTime, outputFile)

