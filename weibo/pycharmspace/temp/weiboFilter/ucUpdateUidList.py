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
from ctypes import *

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

def check():
    pool3 = redis.ConnectionPool(host = "pkm25179.eos.grid.sina.com.cn", port=25179 ,db = 0, decode_responses=True)
    r3 = redis.Redis(connection_pool=pool3)
    pipe3 = r3.pipeline(transaction=False)
    uid, topic = "1001345464", "亚洲新歌榜"
    md5 = hashlib.md5(topic.encode('utf-8')).hexdigest()
    key = uid + "_" + md5
    key = "1930389660_3894638535c3ad212e1dc43e064af2bd"
    print key
    pipe3.hget(key, "1")

    result = pipe3.execute()
    print result


def userTopic2redis():
    lib_handle = cdll.LoadLibrary('./dataRep/libcrc.so')
    outFile = open(sys.argv[2], "w")
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
    with open(sys.argv[1], "r") as inFile:
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
                        redisSet(pipe0, outKey, val)
                    elif partionIndex == 1:
                        redisSet(pipe1, outKey, val)
                    elif partionIndex == 2:
                        redisSet(pipe2, outKey, val)
                    elif partionIndex == 3:
                        redisSet(pipe3, outKey, val)

            if totalNum % 100 == 0:
                pipe0.execute()
                pipe1.execute()
                pipe2.execute()
                pipe3.execute()

                currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                outFile.write("{0}--{1}--{2}--{3}--{4}\n".format(currentTime, totalNum, outKey, partionIndex, line.strip()))
                outFile.flush()
            # 将最后缓存写入
    pipe0.execute()
    pipe1.execute()
    pipe2.execute()
    pipe3.execute()
    outFile.write("totalNum:{0}\n".format(totalNum))
    outFile.write("successfully finished!\n")
    outFile.close()

def redisSet(pipe, outKey, val):
    """
    根据分区写入贡献度值
    :param pipe:
    :param outKey:
    :param val:
    :return:
    """
    if val:
        valArr = val.split("||")
        for elem in valArr:
            elemArr = elem.split(":")
            if len(elemArr) == 2 :
                ucType, num = elemArr[0], int(elemArr[1])
                if ucType == "a":
                    pipe.hset(outKey, "1", num )
                    # pipe.hincrby(outKey, "1", num)
                elif ucType == "b":
                    pipe.hset(outKey, "2", num )
                    # pipe.hincrby(outKey, "2", num)
                elif ucType == "c":
                    if num > 0:
                        pipe.hset(outKey, "3", num)
                        # pipe.hincrby(outKey, "3", num)
                    else:
                        pipe.hset(outKey, "3", 0)
                elif ucType == "va":
                    pipe.hset(outKey, "4", num )
                    # pipe.hincrby(outKey, "4", num)
                elif ucType == "vb":
                    pipe.hset(outKey, "5", num )
                    # pipe.hincrby(outKey, "5", num)

def test():
    lib_handle = cdll.LoadLibrary('./libcrc.so')
    uid = "3865555799"
    partionIndex = lib_handle.RunCRC64(uid, len(uid), 0)
    print partionIndex




if __name__ == "__main__":
    # userTopic2redis()
    check()
    # test()