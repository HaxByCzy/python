#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 话题推荐计算原创有效话题讨论数
@time: 2017-04-02
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

    def processOneWeibo(self, fieldMap):

        if "ARCHIVE_STATUS" in fieldMap and fieldMap["ARCHIVE_STATUS"] == "D":
            sys.stderr.write("reporter:counter:weibo,deleteNum,1\n")
            return None

        # 过滤非原创
        filterTag = fieldMap["FILTER"] if "FILTER" in fieldMap else 0

        if filterTag != "" and (int(filterTag) & 0x4) != 0:
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
                    sys.stdout.write("{0}\t{1}\n".format(topic, str(num)))
        self.topicDict = {}

    def __del__(self):
        self.outputTopic()

if __name__ == "__main__":
    mapper = Mapper()
    for line in sys.stdin:
        mapper.processOneLine(line.strip())
    mapper.flush()

