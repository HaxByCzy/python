#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 将用户有效粉丝数写入redis
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


pikaConf = [{"host" : "rm11350.eos.grid.sina.com.cn", "port" : 11350},
            {"host" : "rm11351.eos.grid.sina.com.cn", "port" : 11351},
            {"host" : "rm11352.eos.grid.sina.com.cn", "port" : 11352},
            {"host" : "rm11353.eos.grid.sina.com.cn", "port" : 11353},
            {"host" : "rm11354.eos.grid.sina.com.cn", "port" : 11354},
            {"host" : "rm11355.eos.grid.sina.com.cn", "port" : 11355},
            {"host" : "rm11356.eos.grid.sina.com.cn", "port" : 11356},
            {"host" : "rm11357.eos.grid.sina.com.cn", "port" : 11357},]

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


def readFile2redis(inFilename, logFilename):
    """
    将文件内容放到字典中
    :param filename:
    :param numDict:
    :return:
    """
    pipeList = initPikaConnection(pikaConf)

    indexNum = 0
    logFile = open(logFilename, "w")
    with open(inFilename, "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            indexNum += 1
            if len(lineArr) == 2:
                uid = int(lineArr[0])
                val = int(lineArr[1])

                partIndex =  uid / 10 % len(pikaConf)
                pipe = pipeList[partIndex]
                pipe.hset(uid, "16", val)

                if indexNum % 1000 == 0:
                    for pipe in pipeList:
                        pipe.execute()
                    currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                    logFile.write("{0}\t{1}--{2}--{3}\n".format(currentTime, indexNum, partIndex, line.strip()))

                    logFile.flush()

    for pipe in pipeList:
        pipe.execute()
    logFile.write("write success {0}".format(indexNum))

    logFile.close()






if __name__ == "__main__":
    inFilename = sys.argv[1]
    logFilename = sys.argv[2]
    readFile2redis(inFilename, logFilename)