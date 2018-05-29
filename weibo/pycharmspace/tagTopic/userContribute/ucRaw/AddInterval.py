#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 将字典文件 写入到redis中
@time: 2017-07-24 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import redis
import time
import hashlib
import memcache
from ctypes import *

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

def check():
    pool3 = redis.ConnectionPool(host = "pkm25177.eos.grid.sina.com.cn", port=25177 ,db = 0, decode_responses=True)
    r3 = redis.Redis(connection_pool=pool3)
    pipe3 = r3.pipeline(transaction=False)
    uid, topic = "1001345464", "亚洲新歌榜"
    md5 = hashlib.md5(topic.encode('utf-8')).hexdigest()
    key = uid + "_" + md5
    key = "5315718090_b943f0c7667000439af7ff3f7f33f531"
    print key
    # pipe3.hget(key, "2")
    pipe3.hmget(key, "1", "7", "3")
    result = pipe3.execute()
    print result

inputHost, inputPort = "10.77.104.194", "11233",
inputKey = "contrib_async_mq"
mc = memcache.Client(["{0}:{1}".format(inputHost, inputPort)])
outFile = open(sys.argv[2], "w")

def userTopic2redis():
    lib_handle = cdll.LoadLibrary('./dataRep/libcrc.so')

    # outFile = open("D://data//uc.log", "w")
    pool0 = redis.ConnectionPool(host = "pkm25177.eos.grid.sina.com.cn", port=25177 ,db = 0, decode_responses=True)
    pool1 = redis.ConnectionPool(host = "pkm25178.eos.grid.sina.com.cn", port=25178 ,db = 0, decode_responses=True)
    pool2 = redis.ConnectionPool(host = "pkm25179.eos.grid.sina.com.cn", port=25179 ,db = 0, decode_responses=True)
    pool3 = redis.ConnectionPool(host = "pkm25180.eos.grid.sina.com.cn", port=25180 ,db = 0, decode_responses=True)
    r0 = redis.Redis(connection_pool=pool0)
    r1 = redis.Redis(connection_pool=pool1)
    r2 = redis.Redis(connection_pool=pool2)
    r3 = redis.Redis(connection_pool=pool3)

    pipe0 = r0.pipeline(transaction=False)
    pipe1 = r1.pipeline(transaction=False)
    pipe2 = r2.pipeline(transaction=False)
    pipe3 = r3.pipeline(transaction=False)


    totalNum = 0
    # with open("D://data//userContribute.dat", "r") as inFile:
    kl0, kl1, kl2, kl3 = [], [], [], []
    with open(sys.argv[1], "r") as inFile:
    # with open("d://data//ucTest.dat", "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            totalNum += 1
            if len(lineArr) == 2:
                key, val = lineArr[0], lineArr[1]
                keyArr = key.split(",,,,")
                if len(keyArr) == 2:
                    uid, topic = keyArr[0], keyArr[1]
                    topicMd5 = hashlib.md5(topic.encode('utf-8')).hexdigest()
                    outKey = "{0}_{1}".format(uid, topicMd5)
                    partionIndex = lib_handle.RunCRC64(outKey, len(outKey), 0) % 4
                    if partionIndex == 0:
                        pipe0.hmget(outKey, "1", "2", "3")
                        kl0.append(outKey)
                    elif partionIndex == 1:
                        pipe1.hmget(outKey, "1", "2", "3")
                        kl1.append(outKey)
                    elif partionIndex == 2:
                        pipe2.hmget(outKey, "1", "2", "3")
                        kl2.append(outKey)
                    elif partionIndex == 3:
                        pipe3.hmget(outKey, "1", "2", "3")
                        kl3.append(outKey)

            if totalNum % 1000 == 0:
                res0 = pipe0.execute()
                writeMc(kl0, res0)
                res1 = pipe1.execute()
                writeMc(kl1, res1)
                res2 = pipe2.execute()
                writeMc(kl2, res2)
                res3 = pipe3.execute()
                writeMc(kl3, res3)
                kl0, kl1, kl2, kl3 = [], [], [], []

                currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                outFile.write("{0}--{1}--{2}--{3}--{4}\n".format(currentTime, totalNum, outKey, partionIndex, line.strip()))
                outFile.flush()
            # 将最后缓存写入
    res0 = pipe0.execute()
    writeMc(kl0, res0)
    res1 = pipe1.execute()
    writeMc(kl1, res1)
    res2 = pipe2.execute()
    writeMc(kl2, res2)
    res3 = pipe3.execute()
    writeMc(kl3, res3)
    kl0, kl1, kl2, kl3 = [], [], [], []
    outFile.write("totalNum:{0}\n".format(totalNum))
    outFile.write("successfully finished!\n")
    outFile.close()

def writeMc(keyList, results):
    for i in range(len(keyList)):
        uid, tid = keyList[i].split("_")
        score = getScore(results[i])
        outLine = "zadd\t{0}\t{1}\t{2}".format(tid, score, uid)
        outFile.write(outLine + "\n")
        mc.set(inputKey, outLine)


def getScore(typeList):
    score = 0
    if typeList[0]:
        score += 5 * int(typeList[0])
    if typeList[1]:
        score += 1 * int(typeList[1])
    if typeList[2]:
        score += int(typeList[2]) / 10
    return score



if __name__ == "__main__":
    userTopic2redis()
    # check()
    # test()