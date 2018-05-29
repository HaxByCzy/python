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
import hashlib
import time
from ctypes import *

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

def writeSuccessTest():
    topic = "杨幂"
    key = getKey(topic)
    print key


def getKey(topic):
    md5=hashlib.md5(topic.encode('utf-8')).hexdigest()
    key = "c_" + md5
    return key

def file2redis2():

    lib_handle = cdll.LoadLibrary('./dataRep/libcrc.so')

    outLogFile = open(sys.argv[2], "w")
    pool0 = redis.ConnectionPool(host = "pkm25165.eos.grid.sina.com.cn", port=25165 ,db = 0, decode_responses=True)
    pool1 = redis.ConnectionPool(host = "pkm25166.eos.grid.sina.com.cn", port=25166 ,db = 0, decode_responses=True)
    r0 = redis.Redis(connection_pool=pool0)
    r1 = redis.Redis(connection_pool=pool1)
    pipe0 = r0.pipeline(transaction=False) # 创建一个管道
    pipe1 = r1.pipeline(transaction=False) # 创建一个管道

    name = "6"
    totalNum = 0

    for line in open(sys.argv[1], "r"):
        totalNum += 1
        lineArr = line.strip().split("\t")
        if len(lineArr) == 2:
            key = getKey(lineArr[0])
            val = int(lineArr[1])
            # pipe.hset(key, name, value=val)
            partionIndex = lib_handle.RunCRC64(key, len(key), 0) % 2
            if partionIndex == 0:
                pipe0.hset(key, name, val)
                # pipe0.hincrby(key, name, val)
            else:
                pipe1.hset(key, name, val)
                # pipe1.hincrby(key, name, val)

        # 输出日志
        if totalNum % 10000 == 0:
            pipe0.execute()
            pipe1.execute()
            currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            outLogFile.write("{0}--{1}--{2}--{3}--{4}\n".format(str(currentTime), str(totalNum), key, partionIndex, str(line.strip())))
            outLogFile.flush()
    pipe0.execute()
    pipe1.execute()
    outLogFile.write("totalNum:{0}".format(str(totalNum)))
    outLogFile.write("sucess !\n")
    outLogFile.close()



if __name__ == "__main__":
    # file2redis2()
    writeSuccessTest()