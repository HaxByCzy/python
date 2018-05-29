#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 将话题用户列表写入pika
@time: 2017-12-12
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


name = "mention"


def file2redis2():
    pool = redis.ConnectionPool(host = "pkm25153.eos.grid.sina.com.cn", port=port ,db = 0, decode_responses=True)
    r = redis.Redis(connection_pool=pool)
    pipe = r.pipeline(transaction=False) # 创建一个管道
    totalNum, errNum = 0, 0

    for line in open("D://data//tmp2.dat"):
    # for line in sys.stdin:
        totalNum += 1
        lineArr = line.strip().split("\t")
        if len(lineArr) == 2:
            key = getKey(lineArr[0])
            valArr = lineArr[1].split(",,")
            uidSet = set([])
            for elem in valArr:
                timeStamp, uid, mid = elem.split("$$")
                member = timeStamp + "_" + mid + "_" + uid
                if uid not in uidSet:
                    pipe.zadd(key, str(member), int(timeStamp))
                    uidSet.add(uid)
        else:
            errNum += 1
            print totalNum ,errNum, " : ", line.strip()
                #每缓冲 n 次向数据库中写一次
        if totalNum % 1000 == 0:
            pipe.execute()
            currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            print currentTime, "----",totalNum,"-----",getKey(lineArr[0]),"---",line.strip()
    # 将缓存数据，写入数据库
    pipe.execute()

    print "totalNum:{0},errNum:{1}".format(totalNum, errNum)
    print "sucess !"


def strQ2B(ustring):
    """全角转半角"""
    rstring = ""
    for uchar in ustring:
        inside_code=ord(uchar)
        if inside_code == 12288:                              #全角空格直接转换
            inside_code = 32
        elif (inside_code >= 65281 and inside_code <= 65374): #全角字符（除空格）根据关系转化
            inside_code -= 65248
        rstring += unichr(inside_code)
    return rstring


def getKey(topic):
    topicNew = strQ2B(unicode(topic.lower()))
    md5=hashlib.md5(topicNew.encode('utf-8')).hexdigest()
    key = "t_" + md5
    return key

def writeSuccessTest():
    name = "mention"

    key = getKey("雪佛兰长沙车展")
    pool = redis.ConnectionPool(host = "pkm25153.eos.grid.sina.com.cn", port=port ,db = 0, decode_responses=True)
    r = redis.Redis(connection_pool=pool)
    pipe = r.pipeline(transaction=False) # 创建一个管道

    member = "1447901032_3910842741905626_3558796373"
    print key
    timestamp = 1447901032
    pipe.zadd(key, member, timestamp)
    result = pipe.execute()
    print result



if __name__ == "__main__":
    # writeSuccessTest()
    file2redis2()