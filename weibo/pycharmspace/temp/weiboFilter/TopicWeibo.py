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
import re
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)



class TagTopic(Parser):
    """
    获取微博的tagTopic
    """
    def __init__(self):
        Parser.__init__(self)
        self.topicSet = set([])

    def readConfIdFile(self):
        """
        读取ID文件，放放set
        :return:
        """
        with open("topic.dat", "r") as inFile:
            for line in inFile:
                topic = line.strip()
                if topic :
                    self.topicSet.add(topic)

    def processOneWeiboModify(self, keyList, fieldMap):
        mid = None

        if "ID" in fieldMap and fieldMap["ID"] != "":
            mid = fieldMap["ID"]
        else:
            sys.stderr.write("reporter:counter:weibo,noMidNum,1\n")
            return None

        if "ARCHIVE_STATUS" in fieldMap and fieldMap["ARCHIVE_STATUS"] == "D":
            sys.stderr.write("reporter:counter:weibo,deleteNum,1\n")

        line = fieldMap["CONTENT"] if "CONTENT" in fieldMap else ""
        filter = fieldMap["FILTER"] if "FILTER" in fieldMap else 0

        if filter != "" and (int(filter) & 0x4) != 0:
            text = fieldMap["TEXT"] if "TEXT" in fieldMap else ""
            if text:
                line += "//@" + text
        else:
            longText = fieldMap["LONGTEXT"] if "LONGTEXT" in fieldMap else ""
            if longText:
                line = longText

        status = self.hasTopic(line)
        tagTopic = ""
        if status:
            tagTopic = self.getTagTopic(line)

        if tagTopic:
            topicArr = tagTopic.split("#")
            for topic in topicArr:
                for elem in topic.split("\t"):
                    if elem and elem in self.topicSet:
                        self.outputWeibo()


    def hasTopic(self, line):
        """
        判断是否有topic
        :param line:
        :return:
        """
        status = True
        prefix = line.find("#")
        if prefix == -1:
            status = False
        else:
            suffix = line.find("#", prefix + 1)
            if suffix == -1:
                status = False
        return status

    def getTagTopic(self, line):
        """
        获取tag topic
        :param line:
        :return:
        """
        def innerTopic(elem):
            """
            获取同一转发内的话题
            :return:
            """
            outLine = ""
            elemArr = re.findall("#([^#]+)#", elem)
            if elemArr:
                for topic in elemArr:
                    if not topic.strip():
                        topic = topic.replace(" ", "&nbsp;")
                    outLine += topic + "\t"
            return outLine.strip()

        tagTopic = ""
        lineArr = line.split("//@")
        allNoneStatus = True    # tagTopic 字段全为空检查
        for elem in lineArr:
            topic = innerTopic(elem)
            tagTopic += str(topic) + "#"
            if allNoneStatus and topic:
                allNoneStatus = False
        if tagTopic.endswith("#"):
            end = len(tagTopic) - 1
            tagTopic = tagTopic[0: end]

        # 如果话题全为空，则认无话题
        if allNoneStatus:
            tagTopic = ""
        return tagTopic



if __name__ == "__main__":
    tt = TagTopic()
    tt.readConfIdFile()
    for line in sys.stdin:
    # for line in open("D://data//testIn.dat", "r"):
        tt.processOneLine(line.strip())
    tt.flush()

