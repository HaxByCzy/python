#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 计算话题有效讨论数，策略如下
        删除微博不计入讨论数。
        过筛选库策略后计入。筛选库策略包含：
        话题出现在1.）原创微博中包含##2.）转发最末端带## ，参与计数。
        微博中，若包含多个话题词，多个话题词都计数。
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

    def __init__(self, day, startTime, endTime):
        Parser.__init__(self)
        self.startTime = day + " " +startTime
        self.endTime = day + " " + endTime
        self.topicDict = {}

    def processOneWeibo(self, fieldMap):

        weiboTime = None

        if "TIME" in fieldMap and fieldMap["TIME"] != "":
            weiboTime = fieldMap["TIME"]
        else:
            return None

        # 过滤10钟之外的话题
        if weiboTime >= self.startTime and weiboTime <= self.endTime:
            pass
        else:
            return

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
    day = sys.argv[1]
    startTime = sys.argv[2]
    endTime = sys.argv[3]
    mapper = Mapper(day, startTime, endTime)
    for line in sys.stdin:
        mapper.processOneLine(line.strip())
    mapper.outputTopic()
    mapper.flush()

