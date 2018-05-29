#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 计算话题用户贡献度, 存量数据
贡献度=5*A+1*B+C/10。
 A原创贡献：发布原创带#话题词#次数。原创博文前两个话题参与计数；话题数>3的话所有的话题都不计。单用户单话题自然日上限，不超过3篇。从第1篇开始及3篇。
 B转发贡献：发布转发最后一级带#话题词#次数。转发微博最后一级转发只计第1个话题，话题数>2的话所有的话题都不计。单用户单话题自然日上限，不超过3篇。从第1篇开始计3篇。
 C转发热度：带话题词微博被转发次数（多级转发均计入）。单篇上限1000次，超出7日不计。（即优质被转发1000次，等于原创20次）
A/B/C均指当前未删除微博，若用户之后删除带#话题词#微博、转发微博，减去对应贡献度。
@time: 2018-03-05
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
sys.path.append("./util")
sys.path.append("../util")
from Parser import Parser
from dateutil.parser import parse
import time

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

class UserContribute(Parser):
    """
    计算话题用户贡献度， map输出 uid,,,话题   \t  讨论数（a/b/c）
    """

    def __init__(self, day, startTime, endTime):
        Parser.__init__(self)
        self.startTime = day + " " +startTime
        self.endTime = day + " " + endTime

    def processOneWeiboModify(self, keyList, fieldMap):

        uid = ""
        if "UID" in fieldMap and fieldMap["UID"] != "":
            uid = fieldMap["UID"]
        else:
            return None

        # 只计算特定时间的话题
        if "TIME" in fieldMap and fieldMap["TIME"] != "":
            weiboTime = fieldMap["TIME"]
            if  weiboTime > self.endTime or weiboTime < self.startTime:
                return None

        filter = 0
        if "FILTER" in fieldMap and fieldMap["FILTER"] != "":
            filter = int(fieldMap["FILTER"])

        rootUid = ""
        if "ROOTUID" in fieldMap and fieldMap["ROOTUID"] != "":
            rootUid = fieldMap["ROOTUID"]

        tagTopic = ""
        if "TAG_TOPIC" in fieldMap and fieldMap["TAG_TOPIC"] != "":
            tagTopic = fieldMap["TAG_TOPIC"]

        if tagTopic == "":
            return None

        hitBase = 0
        if "HIT_BASE" in fieldMap and fieldMap["HIT_BASE"] != "":
            hitBase = int(fieldMap["HIT_BASE"])

        # 被删除的话题，原创话题转发数为负值输出
        if "ARCHIVE_STATUS" in fieldMap and fieldMap["ARCHIVE_STATUS"] == "D":
            fwNum = 0                   # 转发数
            if "FWNUM" in fieldMap and fieldMap["FWNUM"] != "":
                fwNum = int(fieldMap["FWNUM"])
            if fwNum == 0:              # 转发数为0的不计算
                return None
            topicList = self.getTopTopic(tagTopic, layer="last")
            topicNum = len(topicList)
            if filter & 0x4 == 0:       # 处理原创删除时，转发数相应的减少
                if topicNum > 0 and topicNum <= 3:            # 话题数大于4，所有话题都不计
                    for topic in topicList[:2]:
                        sys.stdout.write("{0},,,,{1}\tc:-{2}\n".format(rootUid, topic, fwNum))
            return None

        if filter & 0x4 == 0:       # 处理原创
            topicList = self.getTopTopic(tagTopic, layer="first")
            topicNum = len(topicList)
            if topicNum > 0 and topicNum <= 3:            # 话题数大于3，所有话题都不计
                for topic in topicList[:2]:               # 只前两个话题计数
                    sys.stdout.write("{0},,,,{1}\ta:1\n".format(uid,topic))           # 防止话题中存在逗号，故增个逗号个数
                    if hitBase != 0:
                        sys.stdout.write("{0},,,,{1}\tva:1\n".format(uid,topic))
        else:                       # 处理转发
            # 计算用户自己输入的话题
            topicList = self.getTopTopic(tagTopic, layer="first")
            topicNum = len(topicList)
            if topicNum > 0 and topicNum <= 2:            # 转发话题数大于2，所有话题都不计
                for topic in topicList[:1]:
                    sys.stdout.write("{0},,,,{1}\tb:1\n".format(uid, topic))
                    if hitBase != 0:
                        sys.stdout.write("{0},,,,{1}\tvb:1\n".format(uid,topic))

            # 计算根微博话题
            # 抽取根微博时间与转发微博时间，计算二者时间差，若大于七天，则忽略不计
            rootMid = fieldMap["ROOTMID"] if  "ROOTMID" in fieldMap and fieldMap["ROOTMID"] != "" else ""
            mid = fieldMap["ID"] if  "ID" in fieldMap and fieldMap["ID"] != "" else ""
            rootMidDate = self.getTimeById(rootMid)
            midDate = self.getTimeById(mid)
            interval = self.getDateInterval(rootMidDate, midDate)
            if interval <= 7:       #7天以内的转发有效
                topicList = self.getTopTopic(tagTopic, layer="last")
                topicNum = len(topicList)
                if topicNum > 0 and topicNum <= 3:            # 话题数大于3，所有话题都不计
                    for topic in topicList[:2]:
                        sys.stdout.write("{0},,,,{1}\tc:1\n".format(rootUid, topic))

    def getTimeById(self, mid):
        """
        根据微博ID计算微博发布时间戳
        :param mid:
        :return:
        """
        date = None
        try:
            if len(mid) == 16:
                tmp_time = time.localtime((int(mid) >> 22) + 515483463)
                date = time.strftime("%Y-%m-%d %H:%M:%S",tmp_time)
            else:
                date = None
        except BaseException:
            sys.stderr.write("reporter:counter:weibo,mid2timestampErrNum,1\n")
            date = None
        return date

    def getDateInterval(self, date1, date2):
        """
        计算两个日期的时间差
        :param date1:
        :param date2:
        :return:
        """
        interval = 0
        try:
            if date1 and date2:
                d1 = parse(date1)
                d2 = parse(date2)
                interval = (d2 - d1).days
        except BaseException:
            sys.stderr.write("reporter:counter:weibo,dateSubExcept,1\n")
        return interval


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


if __name__ == "__main__":
    day = sys.argv[1]
    startTime = sys.argv[2]
    endTime = sys.argv[3]
    uc = UserContribute(day, startTime, endTime)
    # for line in open("D://data//atWeibo.txt", "r"):
    for line in sys.stdin:
        uc.processOneLine(line.strip())
    uc.flush()