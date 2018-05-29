#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-04-13 
@author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import redis
import time
from ctypes import *
import hashlib

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

endTime = "2018-06-01 00:00:00"
timeArray = time.strptime(endTime, "%Y-%m-%d %H:%M:%S")
endStamp = int(time.mktime(timeArray))


pikaConf = [{"host" : "rm23700.eos.grid.sina.com.cn", "port" : 23700},
            {"host" : "rm23701.eos.grid.sina.com.cn", "port" : 23701},
            {"host" : "rm23702.eos.grid.sina.com.cn", "port" : 23702},
            {"host" : "rm23703.eos.grid.sina.com.cn", "port" : 23703},
            {"host" : "rm23704.eos.grid.sina.com.cn", "port" : 23704},
            {"host" : "rm23705.eos.grid.sina.com.cn", "port" : 23705},
            {"host" : "rm23706.eos.grid.sina.com.cn", "port" : 23706},
            {"host" : "rm23707.eos.grid.sina.com.cn", "port" : 23707},]

def initPikaConnection(confList):
    """
     根据 pika 的 ip 与端口初始化链接到列表
    :param confList:
    :return:
    """
    connectList = []
    for confDict in confList:
        pool = redis.ConnectionPool(host=confDict["host"], port=confDict["port"] ,db = 0, decode_responses=True)
        r = redis.Redis(connection_pool=pool)
        pipe = r.pipeline(transaction=False)
        connectList.append(pipe)
    return connectList

def getFileName(days):
    nameList = []
    timeStamp = int(time.time())
    hourSecond = 3600
    for i in range(0, days + 1):
        tmpTimeStamp = timeStamp - hourSecond * 24 * i
        timeFormat = time.strftime('%Y-%m-%d',time.localtime(tmpTimeStamp))
        nameList.append(timeFormat)
    return nameList

def readFile2redis(filename, days, pipeList):
    """
    将文件内容放到字典中
    :param filename:
    :param numDict:
    :return:
    """
    lib_handle = cdll.LoadLibrary('./dataSum/libcrc.so')

    indexNum = 0
    logFile = open("./dataLog/{0}.log".format(filename), "w")
    with open("./dataSum/{0}-{1}.dat".format(filename, days), "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            indexNum += 1
            if len(lineArr) == 7:
                topic = lineArr[0]
                topicMd5 = hashlib.md5(topic.encode('utf-8')).hexdigest()
                key = "{0}0000_{1}".format(filename, topicMd5).replace("-", "")
                originVld = int(lineArr[1]) if lineArr[1] != "--" else 0
                originInvld = int(lineArr[2]) if lineArr[2] != "--" else 0
                fwVld = int(lineArr[3]) if lineArr[3] != "--" else 0
                fwInvld = int(lineArr[4]) if lineArr[4] != "--" else 0
                rootVld = int(lineArr[5]) if lineArr[5] != "--" else 0
                rootInvld = int(lineArr[6]) if lineArr[6] != "--" else 0
                partIndex =  lib_handle.RunCRC64(key, len(key), 0) % 4
                pipe = pipeList[partIndex]
                if originVld > 0:
                    # pipe.hincrby(key, "0", originVld)
                    pipe.hset(key, "0", originVld)
                if fwVld > 0:
                    pipe.hset(key, "2", fwVld)
                if rootVld > 0:
                    pipe.hset(key, "3", rootVld)
                if originInvld > 0:
                    pipe.hset(key, "4", originInvld)
                if fwInvld > 0:
                    pipe.hset(key, "6", fwInvld)
                if rootInvld > 0:
                    pipe.hset(key, "7", rootInvld)
                pipe.expireat(key, endStamp)

                if indexNum % 1000 == 0:
                    pipe.execute()
                    currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                    logFile.write("{0}\t{1}--{2}--{3}--{4}\n".format(currentTime, indexNum, partIndex, key, line.strip()))
                    #print filename, key
                    logFile.flush()

    for pipe in pipeList:
        pipe.execute()
    logFile.write("write success {0}".format(indexNum))

    logFile.close()


def increaseOneFile(pipeList, filename, keyDateList):
    lib_handle = cdll.LoadLibrary('./dataSum/libcrc.so')

    indexNum = 0
    logFile = open("./dataLog/{0}.log".format(filename), "w")
    with open("./dataRep/{0}.dat".format(filename), "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            indexNum += 1
            if len(lineArr) == 7:
                topic = lineArr[0]
                topicMd5 = hashlib.md5(topic.encode('utf-8')).hexdigest()

                originVld = int(lineArr[1]) if lineArr[1] != "--" else 0
                originInvld = int(lineArr[2]) if lineArr[2] != "--" else 0
                fwVld = int(lineArr[3]) if lineArr[3] != "--" else 0
                fwInvld = int(lineArr[4]) if lineArr[4] != "--" else 0
                rootVld = int(lineArr[5]) if lineArr[5] != "--" else 0
                rootInvld = int(lineArr[6]) if lineArr[6] != "--" else 0
                for elem in keyDateList:
                    key = "{0}0000_{1}".format(elem, topicMd5)
                    partIndex =  lib_handle.RunCRC64(key, len(key), 0) % 4
                    pipe = pipeList[partIndex]
                    if originVld > 0:
                        pipe.hincrby(key, "0", originVld)
                        # pipe.hset(key, "0", originVld)
                    if fwVld > 0:
                        pipe.hincrby(key, "2", fwVld)
                        # pipe.hset(key, "2", fwVld)
                    if rootVld > 0:
                        pipe.hincrby(key, "3", rootVld)
                        # pipe.hset(key, "3", rootVld)
                    if originInvld > 0:
                        pipe.hincrby(key, "4", originInvld)
                        # pipe.hset(key, "4", originInvld)
                    if fwInvld > 0:
                        pipe.hincrby(key, "6", fwInvld)
                        # pipe.hset(key, "6", fwInvld)
                    if rootInvld > 0:
                        pipe.hincrby(key, "7", rootInvld)
                        # pipe.hset(key, "7", rootInvld)
                    pipe.expireat(key, endStamp)

                    if indexNum % 100 == 0:
                        pipe.execute()
                        currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                        logFile.write("{0}\t{1}--{2}--{3}--{4}\n".format(currentTime, indexNum, partIndex, key, line.strip()))
                        #print filename, key
                        logFile.flush()

    for pipe in pipeList:
        pipe.execute()
    logFile.write("write success {0}".format(indexNum))

    logFile.close()


def addIntervalData():

    inputDate = "2018-04-13"
    pipeList = initPikaConnection(pikaConf)
    keyList = getKeyDate(inputDate)
    increaseOneFile(pipeList, inputDate, keyList)
    # for elem in keyList:
    #     print elem

def getKeyDate(inDate):
    keyList = []
    timeArray = time.strptime(inDate, "%Y-%m-%d")
    timestamp = time.mktime(timeArray)
    for i in range(0, 1):
        a =  int(timestamp) - i * 24 * 3600
        currentTime = time.strftime('%Y%m%d',time.localtime(a))
        keyList.append(currentTime)
    return keyList



def multiTopicNum2redis():
    pipeList = initPikaConnection(pikaConf)
    fileList = getFileName(30)
    for i in range(1, len(fileList)):
        readFile2redis(fileList[i], i, pipeList)



if __name__ == "__main__":
    # multiTopicNum2redis()
    addIntervalData()