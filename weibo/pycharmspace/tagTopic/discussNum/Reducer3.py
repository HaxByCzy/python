#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 合并话题所有uid列表，并输出最终20个近期用户
@time: 2017-12-15
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
    合并所的话题列表，输出最终20个近期用户
    """
    SEP = "\t"

    def __init__(self):
        self.currentKey = None          # 正处理的 key
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
                    self.outputSingleTopicResult()
                    self.topicMidList = []
                    for elem in val.split(",,"):
                        self.topicMidList.append(elem)
                    self.currentKey = key
                else:
                    self.topicMidList = []
                    for elem in val.split(",,"):
                        self.topicMidList.append(elem)
                    self.currentKey = key
            else:
                for elem in val.split(",,"):
                    index = bisect.bisect_left(self.topicMidList, elem)
                    bisect.insort_left(self.topicMidList, elem)
                tmpSize = len(self.topicMidList)
                if tmpSize > 2000:
                    self.topicMidList = self.topicMidList[tmpSize - 1000: tmpSize]

    def outputSingleTopicResult(self):
        """
        输出单个话题的统计结果
        :return:
        """
        sys.stderr.write("reporter:counter:weibo,topicNum,1\n")
        outMidList = self.topicMidList
        outMidList.reverse()

        # 从若干个用户中，提取20个不同的用户
        uidSet = set([])
        finalOutMidList = []
        for elem in outMidList:
            timestamp, mid, uid = elem.split("$$")
            if len(uidSet) >= self.outUidNum:        # 减少输空间，控制用户uid个数
                break
            if uid not in uidSet:
                uidSet.add(uid)
                finalOutMidList.append("{0}$${1}$${2}".format(timestamp,mid,uid))

        # 用户列表输出
        if finalOutMidList:
            latestId = ",,".join(finalOutMidList)
            partBout = "{0}\t{1}\n".format(self.currentKey, latestId)
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
