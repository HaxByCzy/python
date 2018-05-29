#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-04-10 
@author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import redis
import time

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

host = "10.77.104.182"
port = 6382
client = redis.Redis(host=host, port=port, db=0, password="push_statistics")

def reportGene():

    timeStamp = int(time.time())
    fileName = time.strftime('%Y%m%d%H',time.localtime(timeStamp))
    hourSecond = 3600

    keyList = ["GLOBAL_", "GLOBAL_TID_", "GOOD_", "HEADLINE_", "STRATEGY_0_",
               "STRATEGY_1_", "STRATEGY_1_", "STRATEGY_3_", "STRATEGY_4_", "STRATEGY_5_",
                "STRATEGY_6_", "STRATEGY_7_", "STRATEGY_8_", "STRATEGY_9_"]
    allKeyDict = {}
    for elem in keyList:
        allKeyDict[elem] = []

    resultList = []
    for i in range(0, 24):
        tmpTimeStamp = timeStamp -  i * hourSecond
        timeFormat = time.strftime('%Y%m%d%H',time.localtime(tmpTimeStamp))

        valLine = ""
        for elem in keyList:
            hourKeyList = getHourKeys(elem, timeFormat)
            hourNum = len(client.sunion(hourKeyList))
            allKeyDict[elem] += hourKeyList
            valLine += str(hourNum) + "\t"
        resultList.append("{0}\t{1}".format(timeFormat, valLine.strip()))

    outLine = "24小时总体\t"
    for elem in keyList:
        allNum = len(client.sunion(allKeyDict[elem]))
        outLine += str(allNum) + "\t"
    resultList.insert(0, outLine.strip())

    resultList.insert(0, "时间段\tMID\t话题id\t优质话题\t头条统计\t人工干预词\t明星话题词\t热搜话题词\t"
                         "普通热搜词\t标签话题词\t普通话题词\t媒体话题词\t可信话题词\t优质干预话题词\t头条干预话题词")

    outFile = open("./dataRep/" + fileName + ".dat", "w")
    for elem in resultList:
        #print elem
        outFile.write(elem + "\n")
    outFile.close()


def getHourKeys( prefix, hourStamp):
    keyList = []
    mis10Sec = 600
    timestamp = hourStamp + ":00:00"
    timeArray = time.strptime(timestamp, "%Y%m%d%H:%M:%S")
    timestamp = time.mktime(timeArray)
    for i in range(1, 7):
        tmpTimeStamp = timestamp - i * mis10Sec
        timeFormat = prefix + time.strftime('%Y%m%d%H%M',time.localtime(tmpTimeStamp))
        keyList.append(timeFormat)
        # print timeFormat
    return keyList


if __name__ == "__main__":
    reportGene()
