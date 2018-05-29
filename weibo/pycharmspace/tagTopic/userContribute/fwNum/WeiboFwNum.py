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
        self.topicNum = {}
        self.topicDict = {}

    # def readConfIdFile(self):
    #     """
    #     读取ID文件，放放set
    #     :return:
    #     """
    #     with open("topicClass.dat", "r") as inFile:
    #         for line in inFile:
    #             lineArr = line.strip().split("\t")
    #             if len(lineArr) == 2:
    #                 self.topicDict[lineArr[0]] = lineArr[1]

    def processOneWeiboModify(self, keyList, fieldMap):
        mid,uid = None, None
        if "ID" in fieldMap and fieldMap["ID"] != "":
            mid = fieldMap["ID"]
        else:
            return None

        if "UID" in fieldMap and fieldMap["UID"] != "":
            uid = fieldMap["UID"]
        else:
            return None

        filterNum = -1
        if "FILTER" in fieldMap and fieldMap["FILTER"] != "":
            filterNum = int(fieldMap["FILTER"])

        tagTopic = None
        if "TAG_TOPIC" in fieldMap and fieldMap["TAG_TOPIC"] != "":
            tagTopic = fieldMap["TAG_TOPIC"]

        if mid == None or uid == None:
            return None

        fwNum = 0
        if "FWNUM" in fieldMap and fieldMap["FWNUM"] != "":
            fwNum = int(fieldMap["FWNUM"])

        if fwNum == 0:
            sys.stderr.write("reporter:counter:weibo,fwNum0,1\n")
        else:
            sys.stderr.write("reporter:counter:weibo,fwNum,1\n")

        if filterNum & 0x4 == 0:
            tagTopicArr = tagTopic.strip("#").split("\t")
            for topic in tagTopicArr:
                outLine = "{0}\t{1}\t{2}\t{3}\n".format( topic, uid, mid, fwNum)
                sys.stdout.write(outLine)




if __name__ == "__main__":
    map = Mapper()
    # map.readConfIdFile()
    for line in sys.stdin:
    # for line in open("d://data//weibo.dat", "r"):
        map.processOneLine(line.strip())
    map.flush()
