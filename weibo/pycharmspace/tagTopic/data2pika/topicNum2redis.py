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

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

port = 25153
# client = redis.Redis(host = "pkm25153.eos.grid.sina.com.cn", port = port)


def getKey(topic):
    md5=hashlib.md5(topic.encode('utf-8')).hexdigest()
    key = "c_" + md5
    return key


def writeSuccessTest():
    name = "mention"
    topic = "云"
    key = getKey(topic)

    pool = redis.ConnectionPool(host = "pkm25153.eos.grid.sina.com.cn", port=port ,db = 0, decode_responses=True)
    r = redis.Redis(connection_pool=pool)
    pipe = r.pipeline(transaction=False) # 创建一个管道


def file2redis2():
    outLogFile = open(sys.argv[2], "w")
    pool = redis.ConnectionPool(host = "pkm25153.eos.grid.sina.com.cn", port=port ,db = 0, decode_responses=True)
    r = redis.Redis(connection_pool=pool)
    pipe = r.pipeline(transaction=False) # 创建一个管道

    name = "mention"
    totalNum, rightNum,errNum = 0, 0, 0
    maxTopic, maxNum = None, 0

    for line in open(sys.argv[1], "r"):
        totalNum += 1
        lineArr = line.strip().split("\t")
        if len(lineArr) == 2:
            key = getKey(lineArr[0])
            val = int(lineArr[1])
            # pipe.hset(name=name,key=key, value=val)
            pipe.hset(key, name, value=val)
            tmpNum = int(val)
            if tmpNum > maxNum:
                maxNum = tmpNum
                maxTopic = lineArr[0]
                outLogFile.write("{0}\t{1}\t{2}\n".format(maxTopic, maxNum, key))
        else:
            errNum += 1
            outLogFile.write("{0}\t{1}\t{2}\n".format(str(totalNum), str(errNum), line.strip()))
        # 输出日志
        if totalNum % 10000 == 0:
            pipe.execute()
            currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            outLogFile.write("{0}---{1}---{2}---{3}\n".format(str(currentTime), str(totalNum), str(line.strip()), key))
            outLogFile.flush()
    pipe.execute()
    outLogFile.write("totalNum:{0},rightNum:{1},errNum:{2}".format(str(totalNum), str(rightNum), str(errNum)))
    outLogFile.write("maxTopic: {0}, maxNum:{1}\n".format(str(maxTopic), str(maxNum)))
    outLogFile.write("sucess !\n")
    outLogFile.close()



if __name__ == "__main__":
    # file2redis()
    # writeSuccessTest()
    file2redis2()