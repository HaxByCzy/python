#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 读取队列中微博数据，修改redis中数据
@time: 2018-05-17 
@author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import redis
import json
import os
import time
import memcache
sys.path.append("./util")
sys.path.append("../util")
from Parser import Parser

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

redisHost = "10.73.12.132"
redisPort = 6379

redisHost = "pkm25152.eos.grid.sina.com.cn"
redisPort = 25152

redisClient = redis.Redis(host=redisHost, port=redisPort, db=0)

redisPool = redis.ConnectionPool(host = redisHost, port=redisPort ,db = 0, decode_responses=True)
redisR = redis.Redis(connection_pool=redisPool)
redisClientpipe = redisR.pipeline(transaction=False) # 创建一个管道

mc194 = memcache.Client(['10.77.104.194:11233'])
mc195 = memcache.Client(['10.77.104.195:11233'])
mc196 = memcache.Client(['10.77.104.196:11233'])
mc197 = memcache.Client(['10.77.104.197:11233'])
mcKeyName = "huati_zbinfo+sub1"
mcList = [mc194, mc195, mc196, mc197]

field1 = "midaddrepost&xsort=social"
field2 = "midaddrepost&xsort=hot"

class QueueUpdateRedis(Parser):

    def __init__(self):
        Parser.__init__(self)
        self.filename = "./log/redis.log"
        self.logFile = open(self.filename, "w")

    def readQueueWeibo(self):
        """
        读取队列里的微博数据
        :return:
        """
        while True:
            valList = []
            # 读数据
            for mc in mcList:
                val = mc.get(mcKeyName)
                if val != None:
                    valList.append(val)
            if valList:
                for val in valList:
                    self.processValue(val)
            else:
                # print "sleep"
                time.sleep(10)
		self.flush()
                self.logWrite("queue empty !!!!:")

    def processValue(self, val):
        """
        处理一个机器里的数据
        :param val:
        :return:
        """
        if val != None:
            valArr = val.split("\n")
            for line in valArr:
                self.processOneLine(line)

    def processOneWeibo(self, fieldMap):
        tagTmp = fieldMap["TAG_TMP"] if "TAG_TMP" in fieldMap else "{}"
        mid = fieldMap["ID"] if "ID" in fieldMap else ""
        if mid == "":
            return None

        try:
            tagTmpDict = json.loads(tagTmp, encoding='utf8')
            for topic ,value in tagTmpDict.iteritems():
                topicKey = topic + "&istag=2"
                supplyList = value["supply_list"] if "supply_list" in value else []
                for elem in supplyList:
                    uid = int(elem["uid"]) if "uid" in elem else 0
                    timestamp = int(elem["timestamp"]) if "timestamp" in elem else 0
                    # 只处理 act 为2的数据
                    act = int(elem["act"]) if "act" in elem else -1
                    self.logWrite("{0}\tuid={1}\tact={2}\t{3}".format(mid, uid, act, topic))
                    self.logFile.flush()
                    # print "{0}\tuid={1}\tact={2}\n".format(mid, uid, act)
                    if act != 2:
                        return None
                    weiboVal = {"uid" : uid, "timestamp" : timestamp, "act": 2 , "id" : int(mid)}
                    if uid == 0 or timestamp == 0:
                        return None
                    self.logWrite("========{0}\tuid:{1}\tstamp:{2}".format(mid, uid,timestamp))
                    redisField1Val = {}
                    # redisField2Val = {}
                    field1Val = redisClient.hget(topicKey, field1)
                    # field2Val = redisClient.hget(topicKey, field2)
                    if field1Val:
                        redisField1Val[topicKey] = json.loads(field1Val, encoding='utf8')
                    # if field2Val:
                    #     redisField2Val[topicKey] = json.loads(field2Val, encoding='utf8')
                    self.writeRedis(redisField1Val, field1, topicKey, weiboVal)
                    # self.writeRedis(redisField2Val, field2, topicKey, weiboVal)
                    # redisClientpipe.hdel(topicKey, field1)
                    # redisClientpipe.hdel(topicKey, field2)
                    # print "---------------------------------------"
                    # print topicKey
                    # print field1Val
                    # print field2Val
            redisClientpipe.execute()
            self.logFile.write("-----------------------------------------\n")
        except Exception:
            return None

    def writeRedis(self, redisFieldVal, field, topicKey, weiboVal):
        """
        将结果写入redis
        :param redisFieldVal:
        :param field:
        :param topicKeyList:
        :param weiboVal:
        :return:
        """
        if topicKey in redisFieldVal:
            keyList = redisFieldVal[topicKey]
            listSize = len(keyList)
            if listSize < 1000:
                existTag = False
                mid = weiboVal["id"]
                for elem in keyList:
                    if mid == elem["id"]:
                        existTag = True
                if existTag == False:
                    index = 0
                    for i in range(0, listSize):
                        if "act" in keyList[i] and keyList[i]["act"] != 2:
                            index += 1
                        else:
                            break
                    keyList.insert(index,weiboVal)
                    weiboValStr = json.dumps(keyList)
                    redisClientpipe.hset(topicKey, field, weiboValStr)
                    logLine = "{0}\t{1}\t{2}".format(topicKey, field, weiboValStr)
                    self.logWrite(logLine)
        else:
            weiboValStr = json.dumps([weiboVal])
            redisClientpipe.hset(topicKey, field, weiboValStr)
            logLine = "{0}\t{1}\t{2}".format(topicKey, field, weiboValStr)
            self.logWrite(logLine)

    def logWrite(self, line):
        """
        将输入数据写到输出日志里
        :param line:
        :return:
        """
        if os.path.exists(self.filename) == False or os.path.getsize(self.filename) >= 157286400:
            self.logFile = open(self.filename, "w")
        # print line
        currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        self.logFile.write(currentTime + "\t" +line + "\n")
        self.logFile.flush()

    def writeRedisManual(self):
        """
        手动将数据写入redis
        :return:
        """
        key = "喜欢古典文学&istag=2"
        field1 = "midaddrepost&xsort=social"
        field2 = "midaddrepost&xsort=hot"
        value = [{"uid" : "5326480525", "timestamp" : 1526551528, "act": 2 , "id": "4202551869945894"},{"uid" : "5326480525", "timestamp" : 1526541528, "act": 2 , "id": "4188857656522620"}]
        valStr = json.dumps(value)
        redisClient.hset(key, field1, valStr)
        redisClient.hset(key, field2, valStr)


if __name__ == "__main__":
    qur = QueueUpdateRedis()
    qur.readQueueWeibo()
    qur.flush()


