#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 读取队列中用户贡献度，写入到用户贡献度列表中
@time: 2018-02-27 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import memcache
import redis
import time
from ctypes import *
import os

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

# 输入输出配置

subQueueIndex = 0

pikaConf = [{"host" : "pkm25198.eos.grid.sina.com.cn", "port" : 25198},
            {"host" : "pkm25199.eos.grid.sina.com.cn", "port" : 25199},
            {"host" : "pkm25200.eos.grid.sina.com.cn", "port" : 25200},
            {"host" : "pkm25201.eos.grid.sina.com.cn", "port" : 25201},]

inputHost, inputPort = "10.73.12.198", "11233",
inputKey = "contrib_async_mq"
inputKey = inputKey if subQueueIndex == 0 else inputKey + "+sub" + str(subQueueIndex)



def userContributeFromQueue2pika():
    """
    读取队列中贡献度值写入用户贡献度列表pika
    :return:
    """
    lib_handle = cdll.LoadLibrary('./util/libcrc.so')
    # 初始化输入，输出链接
    mc = memcache.Client(["{0}:{1}".format(inputHost, inputPort)])
    connectPoolList = initPikaConnection(pikaConf)

    # 初始化日志文件
    fileTime = time.strftime('%H',time.localtime(time.time()))
    currentLogFilename  = "./log/{0}-{1}.log".format(subQueueIndex, fileTime)
    lastLogFilename = "./log/{0}-{1}.log".format(subQueueIndex, fileTime)
    logFile = open(currentLogFilename, "w")

    cacheCounter, cacheNum = 0, 2000
    # 读取、解释输入队列中数据，并写入到pika中
    while True:
        # 初始化日志变量

        fileTime = time.strftime('%H',time.localtime(time.time()))
        currentLogFilename  = "./log/{0}-{1}.log".format(subQueueIndex, fileTime)
        lastLogFilename = "./log/{0}-{1}.log".format(subQueueIndex, fileTime)

        value = mc.get(inputKey)
        # 当队列已空时等待,并持续监测
        if value == None:
            time.sleep(5)
            # 日志输出
            currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            if not os.path.exists(currentLogFilename):
                if os.path.exists(lastLogFilename): # 关闭上一小时生成文件
                    logFile.close()
                logFile = open(currentLogFilename, "w")
            logFile.write(currentTime + "---queue is empty! program waiting\n")
            logFile.flush()
            continue

        # currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        # logFile.write(" {0}\t{1}\t{2}\n ".format(currentTime, cacheCounter, value))

        valArr = value.strip().split("\t")
        if len(valArr) == 4:
            operate, topicId , score, uid = valArr[0], valArr[1], int(valArr[2]), valArr[3]
            partIndex = lib_handle.RunCRC64(topicId, len(topicId), 0) % len(pikaConf)
            if partIndex == subQueueIndex:
                cacheCounter += 1
                connectPoolList[partIndex].zadd(topicId, uid, score)
                connectPoolList[partIndex].zremrangebyrank(topicId, -120, -101)

            # 将缓存的数据执行写操作, 并输出日志
            if cacheCounter >= cacheNum:
                # 执行写操作
                currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                # logFile.write("{0}\t execute\n".format(currentTime))
                connectPoolList[subQueueIndex].execute()

                # 日志内容输出
                currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                logLine = "{0}--{1}--{2}\n".format(currentTime, value.strip(), partIndex)
                if not os.path.exists(currentLogFilename):
                    if os.path.exists(lastLogFilename): # 关闭上一小时生成文件
                        logFile.close()
                    logFile = open(currentLogFilename, "w")
                logFile.write(logLine)
                logFile.flush()
                cacheCounter = 0    # 清空计数器

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

def readQueue2File():
    host , port = "10.77.104.197", 11233
    mc = memcache.Client(["{0}:{1}".format(host, port)])
    keyName = "contrib_async_mq"
    endStatus = True
    fileName = sys.argv[1]
    with open("./" + fileName, "w") as outFile:
        while endStatus:
            val = mc.get(keyName)
            if val == None:
                endStatus = False
            else:
                outFile.write(val + "\n")


def testWrite():
    mc = memcache.Client(["{0}:{1}".format("10.73.12.132", 11244)])
    num = 0
    with open("D://data//fwNum-dup-all.txt", "r") as inFile:
        for line in inFile:
            num+= 1
            outLine = "\t".join(line.strip().split("\t")[2:])
            mc.set("baoquan3queue", outLine)
            print num, outLine

def caseWrite():

    pool2 = redis.ConnectionPool(host = "pkm25200.eos.grid.sina.com.cn", port=25200 ,db = 0, decode_responses=True)
    r2 = redis.Redis(connection_pool=pool2)
    pipe2 = r2.pipeline(transaction=False)
    key = "af4be206e00ba53ef97795053f57e93f"
    uid = "5303378748"
    val = 79100
    pipe2.zrange(key, 0, 5, withscores=True)
    # pipe2.zadd(key, uid, val)
    res = pipe2.execute()
    print res


if __name__ == "__main__":
    # userContributeFromQueue2pika()
    # readQueue2File()
    # testWrite()
    caseWrite()
