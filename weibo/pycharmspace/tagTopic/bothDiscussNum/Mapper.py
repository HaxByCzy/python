#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 计算话题的讨论数与有效讨论数
@time: 2017-12-11
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
sys.path.append("./util")
sys.path.append("../util")
from Parser import Parser
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

class Mapper(Parser):
    """
    TagTopic计算话题的讨论数
    """

    def __init__(self):
        Parser.__init__(self)
        self.topicDict = {}
        self.allTopicDict = {}

    def processOneWeibo(self, fieldMap):

        if "TAG_TOPIC" in fieldMap:
            oneWeiboTopicSet = set([])      # 一条微博中多个相同话题只输出一次
            topicArr = fieldMap["TAG_TOPIC"].split("#")
            for elem in topicArr:
                for topic in elem.split("\t"):
                    if topic and topic not in oneWeiboTopicSet:
                        oneWeiboTopicSet.add(topic)
                        if topic in self.allTopicDict:
                            self.allTopicDict[topic] += 1
                        else:
                            self.allTopicDict[topic] = 1
            if len(self.allTopicDict) > 10000:
                self.outputAllTopic()

        if "ARCHIVE_STATUS" in fieldMap and fieldMap["ARCHIVE_STATUS"] == "D":
            sys.stderr.write("reporter:counter:weibo,deleteNum,1\n")
            return None


        if "HIT_BASE" in fieldMap and fieldMap["HIT_BASE"] == "0":
            sys.stderr.write("reporter:counter:weibo,hitBase0,1\n")
            return None

        if "TAG_TOPIC" in fieldMap:
            oneWeiboTopicSet = set([])      # 一条微博中多个相同话题只输出一次
            topicArr = fieldMap["TAG_TOPIC"].split("#")
            for elem in topicArr:
                for topic in elem.split("\t"):
                    if topic and topic not in oneWeiboTopicSet:
                        oneWeiboTopicSet.add(topic)
                        if topic in self.topicDict:
                            self.topicDict[topic] += 1
                        else:
                            self.topicDict[topic] = 1
            if len(self.topicDict) > 10000:
                self.outputTopic()

    def outputTopic(self):
        """
        话题输出
        :return:
        """
        if self.topicDict:
            for topic, num in self.topicDict.iteritems():
                if topic:
                    sys.stdout.write("{0}\tv:{1}\n".format(topic, str(num)))
        self.topicDict = {}

    def outputAllTopic(self):
        """
        话题输出
        :return:
        """
        if self.allTopicDict:
            for topic, num in self.allTopicDict.iteritems():
                if topic:
                    sys.stdout.write("{0}\ta:{1}\n".format(topic, str(num)))
        self.allTopicDict = {}

    def __del__(self):
        self.outputTopic()
        self.outputAllTopic()

if __name__ == "__main__":
    mapper = Mapper()
    for line in sys.stdin:
        mapper.processOneLine(line.strip())
    mapper.flush()

