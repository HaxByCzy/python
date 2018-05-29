#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 用效性索引：无发布无效微博用户进行识别
@time: 2017-08-25
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
sys.path.append("./util")
from Strategy import Strategy
import json

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

class Reducer(object):
    """
    reduce 端处理
    """
    SEP = "\t"
    THRESHOLE_LOW = 0.4         # 相似度较低的阀值
    THRESHOLD_HIGH = 0.75       # 相似度高的阀值

    def __init__(self):
        self.currentKey = None          # 存储的当前行
        self.currentKeyList = []        # 当前行的所有val值
        self.stragegy = Strategy()

    def mapperData(self, standardInput):
        """
        将 map 的输出内容作检查，产生遍历生成器
        :param standardInput:
        :return: mapper 端输出 generator
        """
        for line in standardInput:
            line = line.strip()
            if len(line.split(Reducer.SEP)) == 6:
                keyVal = line.split(Reducer.SEP, 1)
                yield keyVal

    def reduce(self, standardInput):
        """
        处理每行内容
        :return:
        """
        for key, val in self.mapperData(standardInput):
        # for key, val in self.mapperData(open("d://data//mapOut.dat")):
            if key != self.currentKey:
                if self.currentKey:
                    self.recoginze(self.currentKey, self.currentKeyList)
                    self.currentKey = key
                    self.currentKeyList = []
                    self.currentKeyList.append(val)
                else:
                    self.currentKey = key
                    self.currentKeyList.append(val)
            else:
                self.currentKeyList.append(val)

    def recoginze(self, key, valList):
        """
        输出符合要求的数据
        :return:
        """

        if key == "null":
            return None

        # 将同一用户的所有数据，按发表时间排序
        valList.sort()

        # 计算微博文本的相似度，并按日期分组
        simValList = self.computeSim(valList)
        dateValDict = self.groupDate(simValList)

        # 如果每天量均小于10条微博，默认为正常用户，不再处理，同时加快效率
        everydayNumTag = self.checkEverydayNum(dateValDict, 15)
        if everydayNumTag:
            return None

        # 获取用户特征
        featureDict = self.getUserFeature(dateValDict)

        # 去除非普的高档用主有
        if featureDict["level"] != 1 or featureDict["rank"] > 10:
            return  None

        # 去除那些有效粉丝量较大，因为此类用户常为正常用户
        if featureDict["valid_fans"] > 100:
            return None

        # 通过检测rtmid转发相同微博，过滤异常用户
        total, origin, rtmid, rtmidPer = self.dupRtmidCheck(dateValDict)
        if rtmidPer < 0.5:
            sys.stderr.write("reporter:counter:weibo,rtmidPerUserNum,1\n")
            sys.stdout.write("{0}\torigin:{1}\trtmid:{2}\tper:{3}\tsize:{4}#B\n".format(key, origin, rtmid, rtmidPer, total))
            for val in valList:
                sys.stdout.write(key + "\t" + val + "#A\n")
            return None

        # 通过文本相似性识别无效用户
        (contentStatusDict, textStatusDict) = self.statusCount(dateValDict)
        invalidDateList = self.stragegy.isValidBySim(contentStatusDict, textStatusDict)
        if len(invalidDateList) >= 2:
            sys.stderr.write("reporter:counter:weibo,textSimUserNum,1\n")
            for elem in invalidDateList:
                for lineArr in dateValDict[elem]:
                    line = "\t".join(lineArr)
                    sys.stdout.write(key + "\t" + line + "#A\n")
            for date in invalidDateList:
                sys.stdout.write(key + "\t" + date + "\tcontent:" + str(contentStatusDict[date]) + "\ttext:" + str(textStatusDict[date]) + "#B\n")
            return None

        # 通过时间间隔来识别无效用户
        frequencyDateList = self.stragegy.isValidByTimeInterval(dateValDict, 35, 20)
        freqTag = False
        if len(frequencyDateList) >= 3:
            freqTag = True
        elif len(frequencyDateList) == 2:
            status , mid = False, 0
            for (date, midNum, threeNum, size) in frequencyDateList:
                featureStatus = self.userFeatureFilter(featureDict, 0, 0, 20, 10)
                mid += midNum
                if featureStatus:
                    status = True
            mid = mid / len(frequencyDateList)
            if status and mid < 15:
                freqTag = True

        # 频繁输出
        if freqTag:
            sys.stderr.write("reporter:counter:weibo,freqOutputUserNum,1\n")
            for (date, midNum, threeNum, size) in frequencyDateList:
                sys.stdout.write("{0}\t{1}\tmid:{2}\tthree:{3}\tsize:{4}#B\n".format(key, date, midNum, threeNum, size))
            for date, valList in dateValDict.iteritems():
                for lineArr in valList:
                    line = "\t".join(lineArr)
                    sys.stdout.write(key + "\t" + line + "#A\n")
            return None

        # 单天处理策略，过滤异常用户
        days = len(dateValDict)
        if days == 1:
            simDate = invalidDateList[0] if len(invalidDateList) == 1 else None
            freqDate = frequencyDateList[0][0] if len(frequencyDateList) == 1 else None
            threeNum  = frequencyDateList[0][2] if len(frequencyDateList) == 1 else None
            oneDayTag = None
            if simDate != None:
                if simDate == freqDate: # 相似且频繁
                    oneDayTag = 1
                else:                   # 仅相似
                    featureStatus = self.userFeatureFilter(featureDict, 0, 0, 5, 20)
                    if featureStatus and len(valList) > 50:
                        oneDayTag = 2
            else:                       # 仅频繁
                if freqDate != None:
                    featureStatus = self.userFeatureFilter(featureDict, 0, 0, 5, 10)
                    if featureStatus and threeNum <= 15 and len(valList) > 30:
                        oneDayTag = 3

            if oneDayTag == 1:
                sys.stderr.write("reporter:counter:weibo,oneDaySimFreqUserNum,1\n")
                date, midNum, threeNum, size =  frequencyDateList[0]
                sys.stdout.write(key + "\t" + simDate + "\toneDay1\tcontent:" + str(contentStatusDict[simDate]) + "\ttext:" + str(textStatusDict[simDate]) + "\t" + "mid:{0}\tthree:{1}\tsize:{2}".format(midNum, threeNum, size) + "#B\n")
                for lineArr in dateValDict[simDate]:
                    line = "\t".join(lineArr)
                    sys.stdout.write(key + "\t" + line + "#A\n")
            elif oneDayTag == 2:
                sys.stderr.write("reporter:counter:weibo,oneDaySimUserNum,1\n")
                sys.stdout.write(key + "\t" + simDate + "\toneDay2\tcontent:" + str(contentStatusDict[simDate]) + "\ttext:" + str(textStatusDict[simDate]) + "#B\n")
                for lineArr in dateValDict[simDate]:
                    line = "\t".join(lineArr)
                    sys.stdout.write(key + "\t" + line + "#A\n")
            elif oneDayTag == 3:
                sys.stderr.write("reporter:counter:weibo,oneDayFreqUserNum,1\n")
                date, midNum, threeNum, size =  frequencyDateList[0]
                sys.stdout.write("{0}\t{1}\toneDay3\tmid:{2}\tthree:{3}\tsize:{4}#B\n".format(key, freqDate, midNum, threeNum, size))
                for lineArr in dateValDict[freqDate]:
                    line = "\t".join(lineArr)
                    sys.stdout.write(key + "\t" + line + "#A\n")

    def statusCount(self, dateValDict):
        """
        对已经按日期分组的数据，统计相似度高、中、低的文本条数
        :param dateValList:
        :return:
        """
        contentDict, textDict = {}, {}
        for date, val in dateValDict.iteritems():
            lowC, midC, highC, eqC = 0, 0, 0, 0
            lowT, midT, highT, eqT = 0, 0, 0, 0
            for valArr in val:
                contentSim = float(valArr[1])
                textSim = float(valArr[2])
                # 统计原创相似度分布
                if contentSim == 1.0:
                    eqC += 1
                if contentSim <= Reducer.THRESHOLE_LOW:
                    lowC += 1
                elif contentSim > Reducer.THRESHOLD_HIGH:
                    highC += 1
                else:
                    midC += 1
                #  统计转发相似度分布
                if textSim == 1.0:
                    eqT += 1
                if textSim < Reducer.THRESHOLE_LOW:
                    lowT += 1
                elif textSim > Reducer.THRESHOLD_HIGH:
                    highT += 1
                else:
                    midT += 1
            total = len(val)
            tmpDictC = {"low": lowC, "mid": midC, "high": highC, "sum": total, "1.0" : eqC}
            tmpDictT = {"low" : lowT, "mid":midT, "high": highT, "sum": total, "1.0" : eqT}
            contentDict[date] = tmpDictC
            textDict[date] = tmpDictT
        return (contentDict, textDict)

    def computeSim(self, valList):
        """
        计算上下两行文本的相似度
        :param valList:
        :return:
        """
        outValList = []
        if valList:
            lastLineArr = valList[0].split(Reducer.SEP)
            lastLineArr.insert(1, "0.0")
            lastLineArr.insert(2, "0.0")
            outValList.append(Reducer.SEP.join(lastLineArr))
            if len(valList) > 1:
                for i in range(1, len(valList)):
                    lineArr = valList[i].split(Reducer.SEP)
                    contentSim = self.stragegy.getSim1(lastLineArr[5], lineArr[3])
                    textSim = self.stragegy.getSim1(lastLineArr[6], lineArr[4])
                    lineArr.insert(1, str(contentSim))
                    lineArr.insert(2, str(textSim))
                    outLine = Reducer.SEP.join(lineArr)
                    outValList.append(outLine)
                    lastLineArr = lineArr
        return outValList

    def groupDate(self, valList):
        """
        将微博内容，按相同日期分组到一起
        :param valList:
        :return: {date: [[valList],[valList]]}
        """
        dateValList = {}
        for elem in valList:
            elemArr = elem.split(Reducer.SEP)
            if elemArr[0] != "null":
                date = elemArr[0][0: 10]
                if date in dateValList:
                    dateValList[date].append(elemArr)
                else:
                    dateValList[date] = [elemArr]
        return dateValList

    def checkEverydayNum(self, dateValDict, num):
        """
        检查一个用户每天微博是是否均小于阀值
        :return:
        """
        status = True
        for date, dateList in dateValDict.iteritems():
            count = len(dateList)
            if count >= num:
                status = False
        return status

    def userFeatureFilter(self, featureDict, valid_fans, bi_follows, fans, rank):
        """
        根据用户特征，检查是否满足被过滤的条件
        :param featureDict:
        :return: True or False
        """
        status = True
        if featureDict["valid_fans"] > valid_fans:
            status = False
        if featureDict["bi_follows"] > bi_follows:
            status = False
        if featureDict["fans"] > fans:
            status = False
        if featureDict["rank"] > rank:
            status = False
        return status

    def getUserFeature(self, dateValDict):
        """
        获取用户的粉丝量、互粉量、有效分丝量
        :return: dict
        """
        featureDict = {}
        for date, valList in dateValDict.iteritems():
            jsonFeature = valList[0][3]
            featureDict = json.loads(jsonFeature, encoding='utf8')
            if featureDict:
                break
        return featureDict

    def dupRtmidCheck(self, dateValDict):
        """
        对重复rtmid进行统计，以过滤重复转发相同微博
        :param dateValDict:
        :return:
        """
        rtmidDict = {}
        rtmidSet = set([])
        totalNum, originNum, rtmidNum = 0, 0, 0
        for date, valList in dateValDict.iteritems():
            totalNum += len(valList)
            for val in valList:
                jsonFeature = val[3]
                featureDict = json.loads(jsonFeature, encoding='utf8')
                if "rtmid" in featureDict:
                    rtmidSet.add(featureDict["rtmid"])
                else:
                    originNum += 1
        rtmidNum = len(rtmidSet)
        rtmidPer = "%.2f" % (float(originNum + rtmidNum) / float(totalNum))
        return (totalNum, originNum, rtmidNum, float(rtmidPer))

    def flush(self):
        """
        判断处理列表中缓存的数据
        :return:
        """
        if self.currentKey:
            self.recoginze(self.currentKey, self.currentKeyList)


if __name__ == "__main__":
    reducer = Reducer()
    reducer.reduce(sys.stdin)
    reducer.flush()
