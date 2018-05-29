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
import time
import hashlib
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
        # 根据本地文件目录和hdfs目录运行环境不同，选择不同的目录
        self.so = cdll.LoadLibrary(r"../util/libtopicid.so") if os.path.exists(r"../util/libtopicid.so") else cdll.LoadLibrary(r"./util/libtopicid.so")

    def processOneWeiboModify(self, keyList, fieldMap):
        mid = None

        if "ID" in fieldMap and fieldMap["ID"] != "":
            mid = fieldMap["ID"]
        else:
            sys.stderr.write("reporter:counter:weibo,noMidNum,1\n")
            return None

        line = fieldMap["CONTENT"] if "CONTENT" in fieldMap else ""
        filter = fieldMap["FILTER"] if "FILTER" in fieldMap else 0
        filterTag = None

        if filter != "" and (int(filter) & 0x4) != 0:
            filterTag = "转发"
            text = fieldMap["TEXT"] if "TEXT" in fieldMap else ""
            if text:
                line += "//@" + text
        else:
            filterTag = "原创"
            longText = fieldMap["LONGTEXT"] if "LONGTEXT" in fieldMap else ""
            if longText:
                line = longText

        status = self.hasTopic(line)
        tagTopic = "null"
        if status:
            tagTopic = self.getTagTopic(line)

        timeStamp = fieldMap["TIME"] if "TIME" in fieldMap else "null"
        if timeStamp == "null":
            try:
                tmp_time = time.localtime((int(id) >> 22) + 515483463)
                timeStamp = time.strftime("%Y-%m-%d",tmp_time)
            except BaseException:
                sys.stderr.write("reporter:counter:weibo,id2time,1\n")

        rootMid = fieldMap["ROOTMID"] if "ROOTMID" in fieldMap else "null"
        rootUid = fieldMap["ROOTUID"] if "ROOTUID" in fieldMap else "null"
        uid = fieldMap["UID"] if "UID" in fieldMap else "null"
        userInfo = fieldMap["INNER_USER_INFO"] if "INNER_USER_INFO" in fieldMap else "null"

        tagTopicArr = tagTopic.split("#")
        for elem in tagTopicArr:
            elemArr = elem.split("\t")
            for topic in elemArr:
                outTopic = self.topicTransform(topic)
                topicMd5 = hashlib.md5(outTopic.encode('utf-8')).hexdigest()
                outlne = "{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\n".format(mid, filter, filterTag, timeStamp, rootMid, rootUid, topic, topicMd5, uid, userInfo)
                sys.stdout.write(outlne)


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
                        outTopic = self.topicTransform(topic)
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
    tt = TagTopic()
    for line in sys.stdin:
    # for line in open("D://data//weibo.dat", "r"):
        tt.processOneLine(line.strip())
    tt.flush()

