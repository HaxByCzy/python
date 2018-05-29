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
    pool3 = redis.ConnectionPool(host = "pkm25201.eos.grid.sina.com.cn", port=25201 ,db = 0, decode_responses=True)
    r3 = redis.Redis(connection_pool=pool3)
    pipe3 = r3.pipeline(transaction=False)
    # uid, topic = "1001345464", "谢娜"
    # md5 = hashlib.md5(topic.encode('utf-8')).hexdigest()
    # lib_handle = cdll.LoadLibrary('./dataRep/libcrc.so')
    # partionIndex = lib_handle.RunCRC64(md5, len(md5), 0) % 4
    # print md5
    # print partionIndex
    pipe3.zremrangebyrank("04333dfc5ad8fcbf42242de968007e", 0, -1)
    res = pipe3.execute()
    print res


def userTopic2redis():
    lib_handle = cdll.LoadLibrary('./dataRep/libcrc.so')
    outFile = open(sys.argv[2], "w")
    # outFile = open("D://data//uc.log", "w")
    pool0 = redis.ConnectionPool(host = "rm24043.eos.grid.sina.com.cn", port=24043 ,db = 0, decode_responses=True)
    pool1 = redis.ConnectionPool(host = "rm24044.eos.grid.sina.com.cn", port=24044 ,db = 0, decode_responses=True)
    pool2 = redis.ConnectionPool(host = "rm24045.eos.grid.sina.com.cn", port=24045 ,db = 0, decode_responses=True)
    pool3 = redis.ConnectionPool(host = "rm24046.eos.grid.sina.com.cn", port=24046 ,db = 0, decode_responses=True)
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
                topic, val = lineArr[0], lineArr[1]
                outKey = hashlib.md5(topic.encode('utf-8')).hexdigest()
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
    # pipe.zremrangebyrank(outKey, 0, -1)
    if val:
        valArr = val.split(",")
        for elem in valArr:
            elemArr = elem.split(":")
            if len(elemArr) == 2 :
                try:
                    uc, uid = int(elemArr[0]), (elemArr[1])
                    pipe.zadd(outKey, uid, uc)
                except BaseException:
                    sys.stdout.write(outKey + "\t" + val + "\n")





if __name__ == "__main__":
    userTopic2redis()
    # check()
    # test()