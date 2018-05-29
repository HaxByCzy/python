#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 根据线上方法计算话题讨论数，策略如下
            原创微博中包含##
            转发链上带##
            转发微博的根微博中带##
            只取前两个话题词计数
            删除微博不去除
            仅针对公开微博，参与计数。
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

    def processOneWeibo(self, fieldMap):
        if "PRIVACY" in fieldMap:
            if fieldMap["PRIVACY"] != "0":
                sys.stderr.write("reporter:counter:weibo,invisibleNum,1\n")
                return None

        if "TAG_TOPIC" in fieldMap:
            topicArr = fieldMap["TAG_TOPIC"].split("#")
            # 根据“只取前两个话题词计数”设置计数器
            oneWeiboTopicNum = 0
            for elem in topicArr:
                for topic in elem.split("\t"):
                    if oneWeiboTopicNum < 2 :
                        if topic:
                            oneWeiboTopicNum += 1
                            if topic in self.topicDict:
                                self.topicDict[topic] += 1
                            else:
                                self.topicDict[topic] = 1
                    else:
                        break
                if oneWeiboTopicNum >= 2:
                    break
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

