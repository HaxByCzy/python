#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 手动修改贡献度列表
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



def errorRemove():
    """
    手动修改贡献度列表
    :return:
    """
    lib_handle = cdll.LoadLibrary('./dataRep/libcrc.so')
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

    errDict = {"李易峰快乐大本营":"1291477752"}

    for topic, uid in errDict.iteritems():
        print topic, uid
        outKey = hashlib.md5(topic.encode('utf-8')).hexdigest()
        partionIndex = lib_handle.RunCRC64(outKey, len(outKey), 0) % 4
        pipe = None
        if partionIndex == 0:
            pipe = pipe0
        elif partionIndex == 1:
            pipe = pipe1
        elif partionIndex == 2:
            pipe = pipe2
        elif partionIndex == 3:
            pipe = pipe3
        pipe.zrem(outKey, uid)

    pipe0.execute()
    pipe1.execute()
    pipe2.execute()
    pipe3.execute()





if __name__ == "__main__":
    errorRemove()