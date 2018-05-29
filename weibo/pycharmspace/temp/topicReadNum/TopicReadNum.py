#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 缩减微博数据，抽取数据，以供分析
@time: 2017-08-22 
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
    根据Uid抽取用户微博
    """
    def __init__(self):
        Parser.__init__(self)
        self.topicDict = {}

    def readConfIdFile(self):
        """
        读取ID文件，放放set
        :return:
        """
        with open("topic.dat", "r") as inFile:
            for line in inFile:
                topic = line.strip()
                self.topicDict[topic] = 0

    def processOneWeiboModify(self, keyList, fieldMap):
        tagTopic = None
        if "TAG_TOPIC" in fieldMap and fieldMap["TAG_TOPIC"] != "":
            tagTopic = fieldMap["TAG_TOPIC"]
        tagTopicArr = tagTopic.split("#")
        for elem in tagTopicArr:
            if elem:
                elemArr = elem.split("\t")
                for topic in elemArr:
                    if topic:
                        if topic in self.topicDict:
                            self.topicDict[topic] += 1

    def outputTopicNum(self):
        if self.topicDict:
            for topic, num in self.topicDict.iteritems():
                if num > 0:
                    sys.stdout.write("{0}\t{1}\n".format(topic, num))

if __name__ == "__main__":
    map = Mapper()
    map.readConfIdFile()
    for line in sys.stdin:
        map.processOneLine(line.strip())
    map.flush()
    map.outputTopicNum()
