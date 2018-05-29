#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 从hbase中导出TAG_TOPIC字段到HDFS
@time: 2017-12-05
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import os
from ctypes import *
sys.path.append("./util")
sys.path.append("../util")
from Parser import Parser
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

class TagTopic(Parser):

    def processOneWeiboModify(self, keyList, fieldMap):
        tagTopic = fieldMap["TAG_TOPIC"] if "TAG_TOPIC" in fieldMap else ""

        topicArr = tagTopic.split("#")
        for topics in topicArr:
            if topics:
                for topic in topics.split("\t"):
                    if topic:
                        sys.stdout.write("{0}\t1\n".format(topic))



if __name__ == "__main__":
    tt = TagTopic()
    for line in sys.stdin:
    # for line in open("D://data//testIn.dat", "r"):
        tt.processOneLine(line.strip())
    tt.flush()

