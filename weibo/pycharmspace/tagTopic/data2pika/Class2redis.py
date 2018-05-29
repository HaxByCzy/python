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
from ctypes import *

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

def check():
    uid, cla, weight = "6340398032", "026", "74.91"
    index = (int(uid) / 10) % 8
    print index
    pool3 = redis.ConnectionPool(host = "rm23363.eos.grid.sina.com.cn", port=23363 ,db = 0, decode_responses=True)
    r3 = redis.Redis(connection_pool=pool3)
    pipe3 = r3.pipeline(transaction=False)
    pipe3.hget(uid, cla)
    result = pipe3.execute()
    print result


def uidClsss2redis():
    lib_handle = cdll.LoadLibrary('./libcrc.so')
    outFile = open(sys.argv[2], "w")
    pool0 = redis.ConnectionPool(host = "rm23360.eos.grid.sina.com.cn", port=23360 ,db = 0, decode_responses=True)
    pool1 = redis.ConnectionPool(host = "rm23361.eos.grid.sina.com.cn", port=23361 ,db = 0, decode_responses=True)
    pool2 = redis.ConnectionPool(host = "rm23362.eos.grid.sina.com.cn", port=23362 ,db = 0, decode_responses=True)
    pool3 = redis.ConnectionPool(host = "rm23363.eos.grid.sina.com.cn", port=23363 ,db = 0, decode_responses=True)
    pool4 = redis.ConnectionPool(host = "rm23364.eos.grid.sina.com.cn", port=23364 ,db = 0, decode_responses=True)
    pool5 = redis.ConnectionPool(host = "rm23365.eos.grid.sina.com.cn", port=23365 ,db = 0, decode_responses=True)
    pool6 = redis.ConnectionPool(host = "rm23366.eos.grid.sina.com.cn", port=23366 ,db = 0, decode_responses=True)
    pool7 = redis.ConnectionPool(host = "rm23367.eos.grid.sina.com.cn", port=23367 ,db = 0, decode_responses=True)
    r0 = redis.Redis(connection_pool=pool0)
    r1 = redis.Redis(connection_pool=pool1)
    r2 = redis.Redis(connection_pool=pool2)
    r3 = redis.Redis(connection_pool=pool3)
    r4 = redis.Redis(connection_pool=pool4)
    r5 = redis.Redis(connection_pool=pool5)
    r6 = redis.Redis(connection_pool=pool6)
    r7 = redis.Redis(connection_pool=pool7)
    pipe0 = r0.pipeline(transaction=False)
    pipe1 = r1.pipeline(transaction=False)
    pipe2 = r2.pipeline(transaction=False)
    pipe3 = r3.pipeline(transaction=False)
    pipe4 = r4.pipeline(transaction=False)
    pipe5 = r5.pipeline(transaction=False)
    pipe6 = r6.pipeline(transaction=False)
    pipe7 = r7.pipeline(transaction=False)

    totalNum, rightNum,errNum = 0, 0, 0
    # with open("D://data//test.dat", "r") as inFile:
    with open(sys.argv[1], "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            totalNum += 1
            if len(lineArr) == 3 and totalNum > 56:     # 跳过前56行hadoop日志
                rightNum += 1
                uid, cla, weight = lineArr[0], lineArr[1], int(float(lineArr[2]))
                partionIndex = lib_handle.RunCRC64(uid, len(uid), 0) % 8
                # partionIndex = (int(uid) / 10) % 8
                if partionIndex == 0:
                    pipe0.hset(uid, cla, weight)
                elif partionIndex == 1:
                    pipe1.hset(uid, cla, weight)
                elif partionIndex == 2:
                    pipe2.hset(uid, cla, weight)
                elif partionIndex == 3:
                    pipe3.hset(uid, cla, weight)
                elif partionIndex == 4:
                    pipe4.hset(uid, cla, weight)
                elif partionIndex == 5:
                    pipe5.hset(uid, cla, weight)
                elif partionIndex == 6:
                    pipe6.hset(uid, cla, weight)
                elif partionIndex == 7:
                    pipe7.hset(uid, cla, weight)
            else:
                errNum += 1

            if totalNum % 10000 == 0:
                pipe0.execute()
                pipe1.execute()
                pipe2.execute()
                pipe3.execute()
                pipe4.execute()
                pipe5.execute()
                pipe6.execute()
                pipe7.execute()
                currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                # outFile.write(str(currentTime) + "---" + str(totalNum) + "----" + line.strip() +  + "===" + str(partionIndex))
                outFile.write("{0}---{1}---{2}---{3}\n".format(currentTime, totalNum, line.strip(), partionIndex))
                outFile.flush()
        outFile.write("totalNum:{0},rightNum:{1},errNum:{2}\n".format(totalNum, rightNum,errNum))
        outFile.close()

def test():
    lib_handle = cdll.LoadLibrary('./libcrc.so')
    uid = "3865555799"
    partionIndex = lib_handle.RunCRC64(uid, len(uid), 0)
    print partionIndex




if __name__ == "__main__":
    # uidClsss2redis()
    # check()
    test()