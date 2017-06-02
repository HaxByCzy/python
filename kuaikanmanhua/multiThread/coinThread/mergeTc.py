# -*- coding: utf-8 -*-

import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
from setting import *
import redis

#合并发送邮件文件

def getDeviceUsers():
    client = redis.Redis(host=redis_host, port=redis_port)
    all = client.get("kk:act:assign:utime:cnt:1:2:0")
    two = client.get("kk:act:assign:utime:cnt:2:2:0")
    one = None
    if all != None and two != None:
        one = all - two
    if one != None and two != None:
        return ",".join(map(str,[one, two]))
    else:
        return "-1,-1"

def mergeOutFile(inputTime, assign, assignUsers, consume, topicUse, outFile):
    assignArr = assign.strip().split(",")
    if len(assignArr) != 4:
        assign = "err,err,err,err"
    if len(str(assignUsers)) > 10:
        assignUsers = "err"
    consumeArr = consume.strip().split(",")
    if len(consumeArr) != 2:
        consume = "err,err"
    topicUseArr = topicUse.strip().split(",")
    if len(topicUseArr) != 2:
        topicUseArr = "err,err"

    deviceUsers = getDeviceUsers()
    with open(outFile + "." +str(inputTime), "w") as tmpFile:
        outLine = ",".join(map(str, [str(inputTime), assign, assignUsers, deviceUsers, consume, topicUse]))
        tmpFile.write(outLine + "\n")


if __name__ == "__main__":
    inputTime = "2017041410"
    assign = "636662,55866,50000,28000"
    assignUsers = "544"
    consume = "25922,62"
    topicUse = "100,200"
    outFile = "D://data//coinTotal.dat"

    # inputTime = sys.argv[1].strip()
    # assign = sys.argv[2].strip()
    # assignUsers = sys.argv[3].strip()
    # consume = sys.argv[4].strip()
    # topicUse = sys.argv[5].strip()
    # outFile = sys.argv[6].strip()

    mergeOutFile(inputTime, assign, assignUsers, consume, topicUse, outFile)




