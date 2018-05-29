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
        # for key, val in self.mapperData(open("d://data//mapOut.dat", "r")):
            if key != self.currentKey:
                if self.currentKey:
                    sys.stderr.write("reporter:counter:weibo,topicNum,1\n")
                    self.outputSingleTopicResult()
                    self.currentKey = key
                    self.tmpDict = {}
                    if val in self.tmpDict:
                        self.tmpDict[val] += 1
                    else:
                        self.tmpDict[val] = 1
                else:
                    self.currentKey = key
                    if val in self.tmpDict:
                        self.tmpDict[val] += 1
                    else:
                        self.tmpDict[val] = 1
            else:
                    if val in self.tmpDict:
                        self.tmpDict[val] += 1
                    else:
                        self.tmpDict[val] = 1

    def outputSingleTopicResult(self):
        """
        输出单个话题的统计结果
        :return:
        """
        if self.tmpDict:
            originVld = self.tmpDict["originVld"] if "originVld" in self.tmpDict else "--"
            originInvld = self.tmpDict["originInvld"] if "originInvld" in self.tmpDict else "--"
            fwVld = self.tmpDict["fwVld"] if "fwVld" in self.tmpDict else "--"
            fwInvld = self.tmpDict["fwInvld"] if "fwInvld" in self.tmpDict else "--"
            rootVld = self.tmpDict["rootVld"] if "rootVld" in self.tmpDict else "--"
            rootInvld = self.tmpDict["rootInvld"] if "rootInvld" in self.tmpDict else "--"
            outLine = "{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\n".format(self.currentKey, originVld, originInvld, fwVld, fwInvld, rootVld, rootInvld)
            sys.stdout.write(outLine)

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
