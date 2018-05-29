#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 将用户近期用户列表写入pika
@time: 2018-05-23 
@author:baoquan3 
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

so = cdll.LoadLibrary("./dataRep/libtopicid.so")
pool = redis.ConnectionPool(host = "pkm25153.eos.grid.sina.com.cn", port=25153 ,db = 0, decode_responses=True)
r = redis.Redis(connection_pool=pool)
pipe = r.pipeline(transaction=False) # 创建一个管道

def file2pika(inFilename, logFilename):
    """
    将用户近期用户列表写入pika
    :return:
    """
    indexNum = 0
    logFile = open(logFilename, "w")
    with open(inFilename, "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            indexNum += 1
            if len(lineArr) == 2:
                key = getKey(lineArr[0])
                valArr = lineArr[1].split(",,")
                uidSet = set([])
                for elem in valArr:
                    timeStamp, mid, uid = elem.split("$$")
                    member = timeStamp  + "_" + mid + "_" + uid
                    if uid not in uidSet:
                        pipe.zadd(key, str(member), int(timeStamp))
                        uidSet.add(uid)
                if indexNum % 1000 == 0:
                    pipe.execute()
                    currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                    logFile.write("{0}\t{1}--{2}--{3}\n".format(currentTime, indexNum, key, line.strip()))
                    logFile.flush()

        pipe.execute()
    logFile.write("write success {0}".format(indexNum))
    logFile.close()

def getKey(topic):
    """
    对话题进行归一化
    :param topic:
    :return:
    """
    topicNew = topicTransform(topic)
    md5 = hashlib.md5(topicNew.encode('utf-8')).hexdigest()
    key = "t_" + md5
    return key


def topicTransform(topic):
    """
    对话题进行全半角、大小写、繁简进行转换
    :param topic:
    :return:
    """
    length = len(topic)
    outTopic =(c_char * (length * 2))()
    so.topic_normalize(topic, outTopic)
    return outTopic.value

if __name__ == "__main__":
    inFilename = sys.argv[1]
    logFilename = sys.argv[2]
    file2pika(inFilename, logFilename)