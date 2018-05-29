#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-02-24 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
sys.path.append("./util")
sys.path.append("../util")
from Parser import Parser
from ctypes import *
import os
import re

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)


class  AllTopic(Parser):
    """
    抽取全量的微博话题词
    """
    def __init__(self):
        Parser.__init__(self)
        # 根据本地文件目录和hdfs目录运行环境不同，选择不同的目录
        self.so = cdll.LoadLibrary(r"../util/libtopicid.so") if os.path.exists(r"../util/libtopicid.so") else cdll.LoadLibrary(r"./util/libtopicid.so")

    def processOneWeiboModify(self, keyList, fieldMap):
        line = ""
        if "CONTENT" in fieldMap and fieldMap["CONTENT"] != "":
            line += fieldMap["CONTENT"]
        if "TEXT" in fieldMap and fieldMap["TEXT"] != "":
            line += fieldMap["TEXT"]
        topicList = self.getTopic(line)
        for topic in topicList:
            sys.stdout.write("{0}\t1\n".format(topic))
            transformTopic = self.topicTransform(topic)
            if topic != transformTopic:
                sys.stdout.write("{0}\t1\n".format(transformTopic))


    def getTopic(self, elem):
        """
        获取微博内话题
        :return:
        """
        topicList, topicSet = [], set([])
        elemArr = re.findall("#([^#]+)#", elem)
        if elemArr:
            for topic in elemArr:
                if not topic.strip():
                    topic = topic.replace(" ", "&nbsp;")
                if topic not in topicSet:
                    topicSet.add(topic)
                    topicList.append(topic)
        return topicList

    def topicTransform(self,topic):
        """
        对话题进行全半角、大小写、繁简进行转换
        :param topic:
        :return:
        """
        length = len(topic)
        outTopic =(c_char * (length * 2))()
        self.so.topic_normalize(topic, outTopic)
        return outTopic.value

if __name__ == "__main__":
    at = AllTopic()
    for line in sys.stdin:
    # for line in open("D://data//testIn.dat", "r"):
        at.processOneLine(line.strip())
    at.flush()