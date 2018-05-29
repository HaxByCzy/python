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

    def processOneWeibo(self, fieldMap):

        tagTopic = fieldMap["TAG_TOPIC"] if "TAG_TOPIC" in fieldMap and fieldMap["TAG_TOPIC"] != "" else None
        if tagTopic == None:
            return None

        if "ARCHIVE_STATUS" in fieldMap and fieldMap["ARCHIVE_STATUS"] == "D":
            sys.stderr.write("reporter:counter:weibo,deleteNum,1\n")
            return None

        hitBase = 0
        if "HIT_BASE" in fieldMap and fieldMap["HIT_BASE"] != "":
            hitBase = int(fieldMap["HIT_BASE"])

        filterTag = fieldMap["FILTER"] if "FILTER" in fieldMap else 0

        if filterTag != "" and (int(filterTag) & 0x4) == 0:
            # 处理原创
            topicList = self.getTopTopic(tagTopic, layer="first")
            if hitBase != 0:
                self.outputTopic(topicList, "originVld")
            else:
                self.outputTopic(topicList, "originInvld")
        else:
            # 处理转发
            topicList = self.getTopTopic(tagTopic, layer="first")
            if hitBase != 0:
                self.outputTopic(topicList, "fwVld")
            else:
                self.outputTopic(topicList, "fwInvld")
            # 处理根微博
            topicList = self.getTopTopic(tagTopic, layer="last")
            if hitBase != 0:
                self.outputTopic(topicList, "rootVld")
            else:
                self.outputTopic(topicList, "rootInvld")



    def getTopTopic(self, tagTopic, layer):
        """
        取按转发层级话题，放入列表中返回
        :param layer:
        :param tagTopic:
        :return:
        """
        topicList = []
        tagTopicArr = tagTopic.split("#")
        layerNum = 0
        if layer == "last":
            topicLayers = len(tagTopicArr)
            layerNum = topicLayers - 1 if topicLayers > 0 else 0
        topic = tagTopic.split("#")[layerNum]
        if topic:
            topicArr = topic.split("\t")
            topicSet = set([])
            for elem in topicArr:
                elem = elem.strip()
                if elem and elem not in topicSet:
                    topicList.append(elem)
                    topicSet.add(elem)
        return topicList

    def outputTopic(self, topicList, tag):
        """
        话题输出
        :return:
        """
        if topicList:
            for elem in topicList:
                sys.stdout.write("{0}\t{1}\n".format(elem, tag))


if __name__ == "__main__":
    mapper = Mapper()
    for line in sys.stdin:
        mapper.processOneLine(line.strip())
    mapper.flush()

