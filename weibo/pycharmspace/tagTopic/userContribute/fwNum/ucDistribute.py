#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 补数据，根据uid和话题词，算出贡献度，写入到队列中
@time: 2018-02-28 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import memcache
import redis
import time
import hashlib
from ctypes import *

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

# 输入输出配置
inputHost, inputPort, inputKey = "10.73.12.198", "11233", "contrib_async_mq"

pikaConf = [{"host" : "pkm25177.eos.grid.sina.com.cn", "port" : 25177},
            {"host" : "pkm25178.eos.grid.sina.com.cn", "port" : 25178},
            {"host" : "pkm25179.eos.grid.sina.com.cn", "port" : 25179},
            {"host" : "pkm25180.eos.grid.sina.com.cn", "port" : 25180},]

def userContribut2queue(inFilename, logFilename):

    lib_handle = cdll.LoadLibrary('./util/libcrc.so')
    # 初始化输入，输出链接
    mc = memcache.Client(["{0}:{1}".format(inputHost, inputPort)])
    connectPoolList = initPikaConnection(pikaConf)
    uidTopicList = []
    logFile = open(logFilename, "w")

    lineIndex = 0
    with open(inFilename, "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            if len(lineArr) >= 2:
                topic, uid  = lineArr[0], lineArr[1]
                topicMd5 = hashlib.md5(topic.encode('utf-8')).hexdigest()
                key = "{0}_{1}".format(uid, topicMd5)
                partionIndex = lib_handle.RunCRC64(key, len(key), 0) % len(pikaConf)
                connectPoolList[partionIndex].hmget(key, "1", "2" , "3")
                results = connectPoolList[partionIndex].execute()
                a= results[0][0] if results[0][0] else 0
                b= results[0][1] if results[0][1] else 0
                c= results[0][2] if results[0][2] else 0
                result = results[0]
                score = getScore(result)
                if a == 0 and b ==0 and c ==0:
                    sys.stdout.write("{0}\t{1}\t{2}\n".format(line.strip(), topicMd5, partionIndex))
                logLine = "{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n".format(topic, uid, score, a, b, c)

                logFile.write(logLine)
                logFile.flush()

    logFile.close()

def getScore(typeList):
    """
    计算贡献度分
    :return:
    """
    score = 0
    if typeList[0]:
        score += 10 * int(typeList[0])
    if typeList[1]:
        score += 5 * int(typeList[1])
    if typeList[2]:
        score += int(typeList[2]) /10
    return score



def test():
    key = "3560470847_eace67c79c66847f6741cdcb5dbcdff9"
    key = "3558922463_d541cd742708cd8313653e01a5ba28dd"
    pool0 = redis.ConnectionPool(host = "pkm25180.eos.grid.sina.com.cn", port=25180 ,db = 0, decode_responses=True)
    r0 = redis.Redis(connection_pool=pool0)
    pipe0 = r0.pipeline(transaction=False)
    pipe0.hmget(key,"1", "2" , "3")
    res = pipe0.execute()
    score = getScore(res[0])
    print score
    print res



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

def clearQueue():
    mc = memcache.Client(["{0}:{1}".format(inputHost, inputPort)])
    tag = True
    while tag:
        val = mc.get(inputKey)
        print val
        if val == None:
            print "queue empty!"
            tag = False

if __name__ == "__main__":
    # inFilename = "d://data//tmp.dat"
    # logFilename = "d://data//output.log"
    inFilename = sys.argv[1]
    logFilename = sys.argv[2]
    userContribut2queue(inFilename, logFilename)
    # test()
    # clearQueue()