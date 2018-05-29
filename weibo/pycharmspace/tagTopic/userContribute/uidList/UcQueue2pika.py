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

pikaConf = [{"host" : "rm24043.eos.grid.sina.com.cn", "port" : 24043},
            {"host" : "rm24044.eos.grid.sina.com.cn", "port" : 24044},
            {"host" : "rm24045.eos.grid.sina.com.cn", "port" : 24045},
            {"host" : "rm24046.eos.grid.sina.com.cn", "port" : 24046},]

inputHost, inputPort = "10.77.104.194", "11233",
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

    fileTime =  time.strftime('%d',time.localtime(time.time()))
    logFileName = "./log/{0}-{1}.log".format(subQueueIndex, fileTime)
    logFile = open(logFileName, "w")

    cacheCounter, cacheNum = 0, 100
    # 读取、解释输入队列中数据，并写入到pika中
    while True:
        # 初始化日志变量


        value = mc.get(inputKey)
        # 当队列已空时等待,并持续监测
        if value == None:
            time.sleep(5)
            # 文件更新
            if os.path.exists(logFileName) == False or os.path.getsize(logFileName) >= 10485760:    # 文件量大于10M则重新写入，即保留10M内容
                logFile.close()
                fileTime =  time.strftime('%d',time.localtime(time.time()))
                logFileName = "./log/{0}-{1}.log".format(subQueueIndex, fileTime)
                logFile = open(logFileName, "w")
            # 日志输出
            currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            logFile.write(currentTime + "---queue is empty! program waiting\n")
            logFile.flush()
            continue

        valArr = value.strip().split("\t")
        if len(valArr) == 4:
            operate, topicId , score, uid = valArr[0], valArr[1], int(valArr[2]), valArr[3]
            partIndex = lib_handle.RunCRC64(topicId, len(topicId), 0) % len(pikaConf)
            if partIndex == subQueueIndex:
                cacheCounter += 1
                connectPoolList[partIndex].zadd(topicId, uid, score)
                connectPoolList[partIndex].zremrangebyrank(topicId, 0, -101)

                # 输出本写队列出的数据，方便日志查找
                currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                logFile.write(" {0}\t{1}\n ".format(currentTime, value))

            # 将缓存的数据执行写操作, 并输出日志
            if cacheCounter >= cacheNum:
                # 执行写操作
                currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                logFile.write(currentTime + " is writing...   ,  " )
                logFile.flush()
                connectPoolList[subQueueIndex].execute()
                currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                logFile.write(currentTime + " is writed\n" )
                logFile.flush()

                # 日志内容输出
                currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                logLine = "{0}--{1}--{2}\n".format(currentTime, value.strip(), partIndex)
                if os.path.exists(logFileName) == False or os.path.getsize(logFileName) >= 10485760:    # 文件量大于10M则重新写入，即保留10M内容
                    logFile.close()
                    fileTime = time.strftime('%d',time.localtime(time.time()))
                    logFileName = "./log/{0}-{1}.log".format(subQueueIndex, fileTime)
                    logFile = open(logFileName, "w")
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



if __name__ == "__main__":
    userContributeFromQueue2pika()