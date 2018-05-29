#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 每十分钟计算一次话题讨论数
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
    def __init__(self, day, startTime, endTime):
        Parser.__init__(self)
        self.startTime = day + " " +startTime
        self.endTime = day + " " + endTime

    def processOneWeiboModify(self, keyList, fieldMap):

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

        tagTopic = None
        if "TAG_TOPIC" in fieldMap and fieldMap["TAG_TOPIC"] != "":
            tagTopic = fieldMap["TAG_TOPIC"]
        else:
            return None
        if tagTopic:
            oneWeiboTopicSet = set([])
            tagTopicArr = tagTopic.split("#")
            for elem in tagTopicArr:
                if elem:
                    elemArr = elem.split("\t")
                    for topic in elemArr:
                        if topic:
                            if topic not in oneWeiboTopicSet:
                                sys.stdout.write(topic + "\t1\n")
                                oneWeiboTopicSet.add(topic)


if __name__ == "__main__":
    day = sys.argv[1]
    startTime = sys.argv[2]
    endTime = sys.argv[3]
    map = Mapper(day, startTime, endTime)
    for line in sys.stdin:
        map.processOneLine(line.strip())
    map.flush()
