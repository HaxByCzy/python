#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 话题养号map端处理
@time: 2017-08-04 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
from math import log

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

class TopicUserReducer(object):
    """
    话题养号 reduce 端处理
    """
    SEP = "\t"

    def __init__(self):
        self.currentKey = None          # 存储的当前行
        self.currentKeyList = []        # 当前行的所有val值

    def mapperData(self, standardInput):
        """
        将 map 的输出内容作检查，产生遍历生成器
        :param standardInput:
        :return: mapper 端输出 generator
        """
        for line in standardInput:
            line = line.strip()
            if len(line.split(TopicUserReducer.SEP)) == 9:
                keyVal = line.split(TopicUserReducer.SEP, 1)
                yield keyVal

    def reduce(self, standardInput):
        """
        处理每行内容
        :return:
        """
        for key, val in self.mapperData(standardInput):
        #for key, val in self.mapperData(open("d://data//tu2.dat")):
            if key != self.currentKey:
                if self.currentKey:
                    self.isTopicUser(self.currentKey, self.currentKeyList)
                    self.currentKey = key
                    self.currentKeyList = []
                    self.currentKeyList.append(val)
                else:
                    self.currentKey = key
                    self.currentKeyList.append(val)
            else:
                self.currentKeyList.append(val)

    def isTopicUser(self, key, valList):
        """
        根据用户的所有微博，判断是否是话题养号用户
        :param key:
        :param valList:
        :return:
        """
        # print key
        # for elem in valList:
        #     print "    ", elem
        # return
        topicDocNum = 0
        noActiveNum = 0
        topicList = []
        for val in valList:
            valArr = val.split(TopicUserReducer.SEP)
            filterNum = int(valArr[6])
            validLike = int(valArr[7])
            topicDoc = self.isTopicDoc(valArr[2], filterNum, validLike)
            if topicDoc:
                topicList.append(topicDoc)
                topicDocNum += 1
            timeArr = valArr[3].split(" ")
            if timeArr[1] <= "08:00:00" and timeArr[1] >= "00:30:00":
                noActiveNum += 1
        # 用户特征
        vfans = int(valArr[0])
        ulevel = int(valArr[1])
        fans = int(valArr[4])
        follows = int(valArr[5])
        dupTopicDocNum = self.maxDupTopicNum(topicList)

        topicDupRatio = float(dupTopicDocNum) / topicDocNum if topicDocNum != 0 else 0.0
        # 用户荣誉度
        userPrideRatio = float(fans) / (fans + follows) if fans != 0 or follows != 0 else 0.0
        # 话题占比率
        topicRatio = float(topicDocNum) / len(valList)
        # 非活跃期微博发布率
        noActiveRatio = float(noActiveNum) / (len(valList) * 7)
        # 用户置信度
        userConfidence = userPrideRatio / (log(float(ulevel), 5) + 1.5)

        # 输出到文件part-*-B中：策略日志
        outB = TopicUserReducer.SEP.join(map(str, [key, dupTopicDocNum, topicDocNum, noActiveNum, len(valList), ulevel, vfans, topicDupRatio, topicRatio, noActiveRatio, userPrideRatio, userConfidence]))
        sys.stdout.write(outB + "#B\n")

        # 话题用户识别策略
        status = False
        if topicDupRatio >= 0.7 or noActiveRatio >= 0.64 or vfans > 1000:
            status = False
        elif ((topicDocNum > 5 and topicRatio > 0.9) or (topicDocNum > 12 and topicRatio > 0.65)):
            if vfans < 250:
                status = True
            elif vfans > 250 and vfans < 500 and userConfidence < 0.6:
                status = True
            elif userConfidence < 0.58:
                status = True
        if status:
            sys.stdout.write("3" + "\t" + key + "#A\n")

    def isTopicDoc(self, content, filter, validLike):
        """
        根据微博的内容，判断微博是否是刷话题微博，若是则返回话题，反之则返回Nonee
        :param content: 微博内容
        :param filter: 过滤字段标志
        :param validLike: 有效点赞数
        :return: 元组（True/Fasle, topic）
        """
        # 无图片无短链无视频无音乐,有效赞>0
        if len(content) == 0 or (((filter & 0x1) != 0) or ((filter & 0x2) != 0)  or ((filter & 0x10) != 0) or ((filter & 0x20) != 0) or validLike != 0):
            return None

        result = None
        beginIndex = content.find("#")
        if beginIndex < 2:
            endIndex = content.find("#", beginIndex + 1)
            # 话题后内容小于70
            if endIndex != -1 and (endIndex - beginIndex > 6) and (len(content) - endIndex - 1 < 210):
                result = content[beginIndex + 1 : endIndex - 1]
        return result

    def maxDupTopicNum(self, topicList):
        """
        根据话题list，计算各话题中重复量最大的次数
        :param topicList: 话题列表
        :return: 最大重次数
        """
        maxDupNum = 0
        if topicList:
            dupNum = 1
            maxDupNum = 1
            topicList.sort(key=lambda elem: elem)
            beginTopic = topicList[0]
            for i in range(1, len(topicList)):
                if beginTopic == topicList[i]:
                    dupNum += 1
                else:
                    beginTopic = topicList[i]
                    maxDupNum = dupNum if dupNum > maxDupNum else maxDupNum
                    dupNum = 1
            maxDupNum = maxDupNum if maxDupNum > dupNum else dupNum
        return maxDupNum

    def flush(self):
        """
        判断处理列表中缓存的数据
        :return:
        """
        if self.currentKey:
            self.isTopicUser(self.currentKey, self.currentKeyList)




if __name__ == "__main__":
    tur = TopicUserReducer()
    tur.reduce(sys.stdin)
    tur.flush()