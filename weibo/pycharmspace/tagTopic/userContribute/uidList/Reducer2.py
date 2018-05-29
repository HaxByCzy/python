#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 计算话题用户贡献度列表
@time: 2018-01-28
author:baoquan3
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

class Reducer(object):
    """
    输出贡献设前top-n个用户
    """
    SEP = "\t"

    def __init__(self):
        self.currentKey = None          # 正处理的 key
        self.uidList = []               # 一个话题对应贡献度前top-n用户

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
                    self.uidList = []
                    for elem in val.split(","):
                        self.uidList.append(elem)
                    self.currentKey = key
                else:
                    self.uidList = []
                    for elem in val.split(","):
                        self.uidList.append(elem)
                    self.currentKey = key
            else:
                for elem in val.split(","):
                    position = self.findInsertPosition(self.uidList, elem)
                    self.uidList.insert(position, elem)
                tmpSize = len(self.uidList)
                if tmpSize > 2000:
                    self.uidList = self.uidList[: 100]

    def findInsertPosition(self,uidList, val):
        """
        二分法寻找列表插入位置
        :return:
        """
        low, high = 0, len(uidList)
        valScore = int(val.split(":")[0])
        if high == 0:
            return low
        while low < high:
            mid = (low + high) / 2
            midValScore = int(uidList[mid].split(":")[0])
            if valScore > midValScore:
                high = mid
            else:
                low = mid + 1
        return low

    def outputSingleTopicResult(self):
        """
        输出单个话题的统计结果
        :return:
        """
        sys.stderr.write("reporter:counter:weibo,topicNum,1\n")
        outLine = ""
        if len(self.uidList) > 100:
            outLine = ",".join(self.uidList[:100])
        else:
            outLine = ",".join(self.uidList)
        if outLine and self.currentKey.strip():
            sys.stdout.write("{0}\t{1}\n".format(self.currentKey, outLine))

    def flush(self):
        """
        判断处理列表中缓存的数据
        :return:
        """
        if self.currentKey:
            self.outputSingleTopicResult()
            self.currentKey = None

if __name__ == "__main__":
    reducer = Reducer()
    reducer.reduce(sys.stdin)
    reducer.flush()
