#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 根据输出一段时间的微博 TAG_TOPIC计算话题讨论数，以及最新的用户列表
        删除微博计入讨论数，只增不减。
        不过筛选库策略，全库中包含即计入。
        话题出现在1.）原创微博中包含##2.）转发链上带##3.）转发微博的根微博中带##，且visible false int 微博可见性为1（所有人能看的微博），参与计数。
        微博中，若包含多个话题词，多个话题词都计数。

        原创中带双#用户
        level 0,1,2用户
@time: 2017-12-13
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
sys.path.append("./util")
sys.path.append("../util")
from Parser import Parser
import time
import json

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

class TopicDiscussNum(Parser):
    """
    计算话题的讨论数，map端输出话题与mid，uid
    """

    def __init__(self):
        Parser.__init__(self)

    def processOneWeiboModify(self, keyList, fieldMap):

        mid = None
        if "ID" in fieldMap and fieldMap["ID"] != "":
            mid = fieldMap["ID"]
        else:
            sys.stderr.write("reporter:counter:weibo,noMidNum,1\n")
            return None

        # 添加用户级别
        level = "3"
        if "INNER_USER_INFO" in fieldMap and fieldMap["INNER_USER_INFO"] != "":
            try:
                userInfo = json.loads(fieldMap["INNER_USER_INFO"], encoding='utf8')["users"][0]
            except BaseException:
                sys.stderr.write("reporter:counter:weibo,jsonDecodeErr,1\n")
                userInfo = ""
            if "level" in userInfo:
                level = str(userInfo["level"])

        uid = None
        if "UID" in fieldMap and fieldMap["UID"] != "":
            uid = fieldMap["UID"]
        else:
            sys.stderr.write("reporter:counter:weibo,noUidNum,1\n")
            return None


        timeStamp = None
        if "TIME" in fieldMap and fieldMap["TIME"] != "":
            # 原微博中存在时间格式异常情况，如 20117-11-00 等 ,作特殊处理，
            try:
                timeArray = time.strptime(fieldMap["TIME"], "%Y-%m-%d %H:%M:%S")
                timeStamp = int(time.mktime(timeArray))
            except BaseException:
                sys.stderr.write("reporter:counter:weibo,time2stampErrNum,1\n")
                try:
                    tmp_time = time.localtime((int(mid) >> 22) + 515483463)
                    dt = time.strftime("%Y-%m-%d %H:%M:%S",tmp_time)
                    timeArray = time.strptime(dt, "%Y-%m-%d %H:%M:%S")
                    timeStamp = int(time.mktime(timeArray))
                except BaseException:
                    sys.stderr.write("reporter:counter:weibo,mid2timestampErrNum,1\n")
                    timeStamp = None
        else:
            return None

        val = None
        if mid != None and uid != None and timeStamp != None:
            val = "{0}$${1}$${2}".format(str(timeStamp), mid, uid)

        origin = "y"    # 默认微博为原创，即用户自己写的那部分
        oneWeiboTopicSet = set([])
        if val:
            if "TAG_TOPIC" in fieldMap:
                layer = 0
                topicArr = fieldMap["TAG_TOPIC"].split("#")
                for elem in topicArr:
                    if layer > 0:       # 对转发层的第一层进行用户列表计数
                        origin = "n"
                    for topic in elem.split("\t"):
                        # 同一微博中相同话题，只作一次计算
                        if topic and topic not in oneWeiboTopicSet:
                            sys.stdout.write("{0}\t{1}\n".format(topic, "{0}_{1}_{2}".format(val,origin,level)))
                            oneWeiboTopicSet.add(topic)
                    layer += 1

if __name__ == "__main__":
    tdm = TopicDiscussNum()
    for line in sys.stdin:
    # for line in open("D://data//part-00059", "r"):
        tdm.processOneLine(line.strip())
    tdm.flush()

