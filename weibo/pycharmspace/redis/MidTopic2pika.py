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

pikaConf = [{"host" : "pkm25163.eos.grid.sina.com.cn", "port" : 25163},
            {"host" : "pkm25164.eos.grid.sina.com.cn", "port" : 25164}]

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
    lib_handle = cdll.LoadLibrary('./dataRep/libcrc.so')
    indexNum = 0
    logFile = open(logFilename, "w")
    with open(inFilename, "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            indexNum += 1
            if len(lineArr) == 2:
                mid = lineArr[0]
                topics = lineArr[1]

                value4, value14 = "", ""
                if topics != "noTopicMid":
                    topicsArr = topics.split("==++==baoquan3")
                    for elem in topicsArr:
                        tid = hashlib.md5(elem.encode('utf-8')).hexdigest()
                        value4 += "{0}:,".format(tid)
                        value14 += "{0}##".format(elem)
                    value4 = value4.strip(",")
                    value14 = value14.strip("##")

                partIndex =  lib_handle.RunCRC64(mid, len(mid), 0) % len(pikaConf)
                pipe = pipeList[partIndex]
                pipe.hset(mid, "4", value4)
                pipe.hset(mid, "14", value14)
                pipe.expireat(mid, endStamp)

                if indexNum % 1 == 0:
                    for pipe in pipeList:
                        pipe.execute()
                    currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                    logFile.write("{0}\t{1}--{2}--{3}\n".format(currentTime, indexNum, partIndex, line.strip()))
                    # print "{0}\t{1}--{2}--{3}".format(currentTime, indexNum, partIndex, line.strip())
                    # print "---------------------------------"

                    logFile.flush()

    for pipe in pipeList:
        pipe.execute()
    logFile.write("write success {0}\n".format(indexNum))

    logFile.close()






if __name__ == "__main__":
    inFilename = sys.argv[1]
    logFilename = sys.argv[2]
    # inFilename = "d://data//testIn.dat"
    # logFilename = "d://data//outRedis.log"
    readFile2redis(inFilename, logFilename)