#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 话题讨论数计算，按行计算讨论数，并输出20个近期用户
@time: 2017-12-13
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import bisect

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

class Reducer(object):
    """
    按行计算讨论数，并输出20个近期用户
    """
    SEP = "\t"

    def __init__(self):
        self.currentKey = None          # 正处理的 key
        self.topicTotalNum = 0          # 所有话题总数
        self.singleTopicNum = 0         # 一个单个话题的总讨论数
        self.topicMidList = []          # 一个话题对应的最近mid列表
        self.outUidNum = 20             # 输出保存存近期用户个数

    def mapperData(self, standardInput):
        """
        将 map 的输出内容作检查，产生遍历生成器
        :param standardInput:
        :return: mapper 端输出 generator
        """
        for line in standardInput:
            line = line.strip()
            if len(line.split(Reducer.SEP)) == 2:
                keyVal = line.split(Reducer.SEP, 1)
                yield keyVal

    def reduce(self, standardInput):
        """
        处理每行内容
        :return:
        """
        for key, val in self.mapperData(standardInput):
        # for key, val in self.mapperData(open("d://data//mapOut.dat", "r")):
            if key != self.currentKey:
                if self.currentKey:
                    sys.stderr.write("reporter:counter:weibo,topicNum,1\n")
                    self.outputSingleTopicResult()
                    self.topicMidList = [val] if self.checkUid(val.split("$$")[2]) else []
                    self.singleTopicNum = 1
                    self.currentKey = key
                else:
                    self.topicMidList = [val] if self.checkUid(val.split("$$")[2]) else []
                    self.singleTopicNum = 1
                    self.currentKey = key
            else:
                self.singleTopicNum += 1
                # 将符合要求的用户加入列表
                if self.checkUid(val.split("$$")[2]):
                    index = bisect.bisect_left(self.topicMidList, val)
                    bisect.insort_left(self.topicMidList, val)
                tmpSize = len(self.topicMidList)
                if tmpSize > 5000:
                    self.topicMidList = self.topicMidList[tmpSize - 2000: tmpSize]

    def checkUid(self,uid):
        """
        对用户是所发话题，是否是原创，用户级别是否是 1，2进行检查
        :param uid:
        :return:
        """
        status = False
        outUid, origin, level = uid.split("_")
        # if origin == "y" and int(level) < 3:
        if origin == "y":
            status = True
        return status

    def outputSingleTopicResult(self):
        """
        输出单个话题的统计结果
        :return:
        """
        tmpSize = len(self.topicMidList)
        outMidList = []
        if tmpSize > 2000:
            outMidList = self.topicMidList[tmpSize - 500: tmpSize]
            outMidList.reverse()
        else:
            outMidList = self.topicMidList
            outMidList.reverse()

        # 从若干个用户中，提取20个不同的用户
        uidSet = set([])
        finalOutMidList = []
        for elem in outMidList:
            timestamp, mid, uid = elem.split("$$")
            if len(uidSet) >= self.outUidNum:        # 减少输空间，控制用户uid个数
                break
            outUid = uid.split("_")[0]
            if outUid not in uidSet:
                uidSet.add(outUid)
                finalOutMidList.append("{0}$${1}$${2}".format(timestamp,mid,outUid))

        # 讨论数和用户列表输出
        partAout = "{0}\t{1}#A\n".format(self.currentKey, str(self.singleTopicNum))
        sys.stdout.write(partAout)
        if finalOutMidList:
            latestId = ",,".join(finalOutMidList)
            partBout = "{0}\t{1}#B\n".format(self.currentKey, latestId)
            sys.stdout.write(partBout)

    def flush(self):
        """
        判断处理列表中缓存的数据
        :return:
        """
        self.outputSingleTopicResult()

if __name__ == "__main__":
    reducer = Reducer()
    reducer.reduce(sys.stdin)
    reducer.flush()
