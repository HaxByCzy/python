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

keySet = set([
"HIT_BASE",
"INNER_USER_INFO",
"ACTION",
"ROOTUID",
"ROOTMID",
"TAG_TOPIC",
"TEXT",
"RTMID",
"ADV",
"AUDIT",
"BLACKLIKENUM",
"CITY",
"CMTNUM",
"COLONIAL_ACTION",
"COMMENT_ACTION",
"CONTENT",
"CONT_SIGN",
"DEFLEN",
"DPICNUM",
"DUP",
"DUP_CLUSTER",
"DUP_CONT",
"DUP_URL",
"FILTER",
"FWNUM",
"GROUP_CHEAT",
"ID",
"IDENTIFY_TYPE",
"IDXLEN",
"IDXTEXT",
"LABLE_WORD",
"LEVEL",
"LIKENUM",
"LOW_QUALITY_LTITLE",
"NICK",
"NON_TOPIC_WORDS",
"ORIGINAL",
"PERSONAL_ACTION",
"PRIVACY",
"PROVINCE",
"QI",
"SOURCE",
"SPECIAL_STATE",
"SPICNUM",
"STATUS",
"TAG",
"TERMSWEIGHT",
"TIME",
"TOPIC_ENTITY_WORDS",
"TOPIC_RELATED_WORDS",
"TOPIC_WORDS",
"TW_USERTEXT",
"UID",
"UNRNUM",
"UNVIFWNM",
"URL",
"USERTEXT",
"USER_TYPE",
"VALID_FANS_LEVEL",
"VALIDFWNM",
"VERIFIED",
"VERIFIEDTYPE",
"VIDEO_AUTORUN",
"WHITELIKENUM",
"WHITE_WEIBO",
"WORDS",
"ZONE",
"ARCHIVE_STATUS",
])

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
            tagTopic = self.topicTransform(self.getTagTopic(line))

        # 修改输出内容
        if "ACTION" not in keyList:
            keyList.insert(1, "ACTION")
        fieldMap["ACTION"] = "A"

        if "TAG_TOPIC" not in keyList:
            keyList.insert(2, "TAG_TOPIC")
        fieldMap["TAG_TOPIC"] = tagTopic

        # 输出
        if tagTopic:    # 没有tag_topic字段则不输出
            sys.stderr.write("reporter:counter:weibo,totalOutputNum,1\n")
            sys.stdout.write("@\n")
            for key in keyList:
                if key in keySet and key in fieldMap and fieldMap[key] != "":
                    sys.stdout.write("@{0}:{1}\n".format(key, fieldMap[key]))
        else:
            sys.stderr.write("reporter:counter:weibo,noTagTopicNum,1\n")

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
    # for line in open("D://data//testIn.dat", "r"):
        tt.processOneLine(line.strip())
    tt.flush()

