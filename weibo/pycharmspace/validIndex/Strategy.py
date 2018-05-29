#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2017-08-26 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys

import datetime
import time
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)


class Strategy(object):

    def getSim1(self, text1, text2):
        """
        Jaccard计算两条文本的相似度
        :param text1:
        :param text2:
        :return: 相似度
        """
        if text1 == "转发微博" or text1 == "轉發微博" or text2 == "转发微博" or text2 == "轉發微博":
            return 0.0
        if text1 == "null" or text2 == "null":
            return 0.0
        if text1 == "" and text2 == "":
            return 0.0
        if self.isImageCase(text1.strip()) or self.isImageCase(text2.strip()):
            return 0.0
        s1, s2 = set(unicode(text1)), set(unicode(text2))
        t1, t2 = s1 & s2, s1 | s2
        l1, l2 = len(t1), len(t2)
        sim = float(l1) / l2
        return float("%.4f" % sim)

    def isImageCase(self, text):
        """
        处理内容为 “分享图片” 特殊 case 的文本相似度
        :param text:
        :return: True or False
        """
        status = False
        if (len(unicode(text)) == 7 and "分享图片" in text) or  (len(unicode(text)) == 7 and "分享圖片" in text):
            status = True
        elif text.startswith("分享图片 <sina:link src") and len(unicode(text)) < 100 :
            status = True
        return status

    def isValidByTotalCount(self, dateValDict):
        """
        通过用户在一天内发布的微博总量，太大则记之为无效用户
        :param valList:
        :return: 某日期是否太大
        """
        exceptDateList = []
        for date, dateList in dateValDict.iteritems():
            count = len(dateList)
            if count > 200:
                exceptDateList.append((date, count))
        return exceptDateList

    def isValidBySim(self, contentStatusDict, textStatusDict):
        """
        根据统计情况，识别被频繁发微博的日期
        :param dateStatusDict:
        :return: 返回无效索引微博的日期
        """
        dateList = []
        for date, dateDict in contentStatusDict.iteritems():
            status = False
            total = dateDict["sum"]
            high = dateDict["high"]
            if total >= 15:
                if dateDict["1.0"] > (total / 3) or textStatusDict[date]["1.0"] > (total / 3):
                    status = True
                if high >= (total / 2):
                    status = True
                else:
                    total = textStatusDict[date]["sum"]
                    high = textStatusDict[date]["high"]
                    if high >= (total / 2):
                        status = True
            if status:
                dateList.append(date)
        return dateList

    def isValidByTimeInterval(self, dateValDict, sizeThreshold, freqThreshold):
        """
        计算一个用户发表微博的时间间隔，如果太小，则以异常用户记
        规则为：取时间间隔的中位数和三分之二中位数平均值，若小于阀值，则记之
        :param dateValList:
        :return:
        """
        outDateList = []
        for date, dateList in dateValDict.iteritems():
            size = len(dateList)
            if size > sizeThreshold:
                midNum, threeNum = 0, 0
                lastTimeStamp = int(time.mktime(time.strptime(dateList[0][0], "%Y-%m-%d %H:%M:%S")))
                intervalList = []
                for i in range(1, size):
                    timeStamp = int(time.mktime(time.strptime(dateList[i][0], "%Y-%m-%d %H:%M:%S")))
                    interval = timeStamp - lastTimeStamp
                    intervalList.append(interval)
                    lastTimeStamp = timeStamp
                intervalList.sort()
                midNum , threeNum = intervalList[size / 2], intervalList[2 * (size / 3)]
                means = (midNum + threeNum) / 2
                if means > 0  and means < freqThreshold:
                    outDateList.append((date, midNum, threeNum, size))
        return outDateList

if __name__ == "__main__":
    pass
