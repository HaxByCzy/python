#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function:  话题讨论数与线上讨论数对比
@time: 2017-12-18 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import urllib2
import sys
from ctypes import *
import hashlib
import json

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)


def compare():
    """
    对比讨论数
    :return:
    """

    baseUrl = url = "http://i2.api.weibo.com/2/darwin/topic/count.json?source=2936099636&ids=1022:100808"
    totalNum, errNum = 0, 0
    with open(sys.argv[1], "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            totalNum += 1
            if len(lineArr) == 2:
                if totalNum % 2 == 0:
                    topic = lineArr[0]
                    num = int(lineArr[1])
                    md5 = hashlib.md5(topic.encode('utf-8')).hexdigest()
                    req = urllib2.Request(baseUrl + md5)
                    response = urllib2.urlopen(req)
                    onlineJsonRes = json.loads(response.read(), encoding='utf8')[0]
                    onlineMe = int(onlineJsonRes["me"])
                    diff = num - onlineMe
                    percent = 1.0 * diff / onlineMe if onlineMe != 0 else 1.0
                    outPer = "%.2f" % percent
                    print "{0}\t{1}\t{2}\t{3}\t{4}".format(topic, str(num),str(onlineMe),str(diff), str(float(outPer) * 100) + " %")

            else:
                errNum += 1

            if totalNum >= 1500:
                break
        print "totalNum:", totalNum,"errNum:",errNum

def compare2():
    """
    对比讨论数
    :return:
    """
    topicDict = {}
    outLogFile = open(sys.argv[3], "w")
    with open(sys.argv[1], "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            if len(lineArr) == 3:
                topic = lineArr[0]
                num = int(lineArr[1])
                topicDict[topic] = num

    with open(sys.argv[2], "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            if len(lineArr) == 2:
                topic = lineArr[0]
                num = int(lineArr[1])
                if topic in topicDict:
                    outLine = "{0}\t{1}\t{2}\n".format(topic, topicDict[topic], num)
                    outLogFile.write(outLine)
                    outLogFile.flush()

    outLogFile.close()




if __name__ == "__main__":
    # compare()
    compare2()
