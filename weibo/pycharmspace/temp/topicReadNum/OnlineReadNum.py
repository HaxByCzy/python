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
from ctypes import *
import hashlib
import json
import time

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)


def compare():
    """
    对比讨论数
    :return:
    """
    outFile = open(sys.argv[2], "w")
    # outFile = open("D://data//topicReadNum.dat", "w")

    baseUrl = url = "http://i2.api.weibo.com/2/darwin/topic/count.json?source=2936099636&ids=1022:100808"
    totalNum, errNum = 0, 0
    with open(sys.argv[1], "r") as inFile:
    # with open("d://data//randomTopic.dat", "r") as inFile:
        for line in inFile:
            topicArr = line.strip().split("\t")
            if len(topicArr) == 2:
                topic, num = topicArr[0], topicArr[1]
                md5 = hashlib.md5(topic.encode('utf-8')).hexdigest()
                req = urllib2.Request(baseUrl + md5)
                response = urllib2.urlopen(req)
                onlineJsonRes = json.loads(response.read(), encoding='utf8')[0]
                if "re" in onlineJsonRes:
                    re= int(onlineJsonRes["re"])
                    # ure = int(onlineJsonRes["ure"])
                    outFile.write("{0}\t{1}\n".format(topic, re))
                    outFile.flush()
            else:
                errNum += 1
    print "end"
    outFile.close()

def test():
    """
    对比讨论数
    :return:
    """
    outFile = open("d://data//outFilter", "w")
    with open("d://data/out.dat", "r") as inFile:
        for line in inFile:
            topicArr = line.strip().split("\t")
            if len(topicArr) == 3:
                try:
                    topic= topicArr[0]
                    reNum = int(topicArr[1])
                    ureNum = int(topicArr[2])
                    if len(unicode(topic)) < 50:
                        if reNum > 1000000 and ureNum > 1000000:
                            outFile.write(line)
                            print line.strip()
                except BaseException:
                    print line.strip(), "-------------------------"
    outFile.close()



if __name__ == "__main__":
    compare()
    # test()