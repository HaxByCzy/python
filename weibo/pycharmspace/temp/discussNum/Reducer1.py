#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: reduce端,计算所有话题的讨论数
@time: 2017-12-11
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
    计算所有话题的讨论数
    """
    SEP = "\t"

    def __init__(self):
        self.currentKey = None          # 正处理的 key
        self.tmpDict = {}

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
        # for key, val in self.mapperData(open("d://data//mapOut1.dat", "r")):
            if key != self.currentKey:
                if self.currentKey:
                    sys.stderr.write("reporter:counter:weibo,allTopic,1\n")
                    self.outputSingleTopicResult()
                    self.currentKey = key
                    self.tmpDict = {}
                    self.keyProcess(key)
                    self.valProcess(key, val)
                else:
                    self.currentKey = key
                    self.keyProcess(key)
                    self.valProcess(key, val)
            else:
                    self.keyProcess(key)
                    self.valProcess(key, val)

    def keyProcess(self, key):
        if key in self.tmpDict:
            self.tmpDict[key] += 1
        else:
            self.tmpDict[key] = 1

    def valProcess(self,key ,val):
        if self.tmpDict[key] < 2000:
            valArr = val.split("==zhangsan&&lisi++wangwu--")
            if len(valArr) == 3:
                topicLen = len(unicode(valArr[0]))
                if topicLen < 15 and "]" not in valArr[0]:
                    outLine = "{0}\t{1}\t{2}\t{3}#B".format(key, valArr[0], valArr[1], valArr[2])
                    sys.stdout.write(outLine + "\n")

    def outputSingleTopicResult(self):
        """
        输出单个话题的统计结果
        :return:
        """
        if self.tmpDict:
            outLine = "{0}\t{1}#A".format(self.currentKey, self.tmpDict[self.currentKey])
            sys.stdout.write(outLine + "\n")


    def flush(self):
        """
        判断处理列表中缓存的数据
        :return:
        """
        if self.currentKey:
            self.outputSingleTopicResult()


if __name__ == "__main__":
    reducer = Reducer()
    reducer.reduce(sys.stdin)
    reducer.flush()
