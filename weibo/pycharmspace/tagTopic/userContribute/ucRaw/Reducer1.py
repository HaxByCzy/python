#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 计算话题用户贡献度, 存量数据
A : 原创微博前两个话题计入，话题数>4的话所有的话题都不计
B : 转发微博最后一级转发只计第1个话题，话题数>2的话所有的话题都不计
C : 转发行为，给原创计数；删除进行减计；对于aaa//@bbb #111#//@ccc//@ddd #222#这条微博对于bbb对@111#和ddd对#222#的贡献度都计数；上面两条堆砌限制仍然有效
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
        self.typeDict = {}              # 不同类型计数字典

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
                    self.outputTopicResult()
                    self.currentKey = key
                    self.typeDict = {}
                    self.topicStatistic(val)
                else:
                    self.currentKey = key
                    self.topicStatistic(val)
            else:
                self.topicStatistic(val)

    def topicStatistic(self, val):
        """
        对话题不同类型进行统计
        :param val:
        :return:
        """
        for elem in val.split("||"):
            elemArr = elem.split(":")
            if len(elemArr) == 2:
                topic, num = elemArr[0], int(elemArr[1])
                if topic in self.typeDict:
                    self.typeDict[topic] += num
                else:
                    self.typeDict[topic] = num

    def valCheck(self):
        """
        对单天A,B,C的单天数量进行检查，A,Bh于3，C小于1000，若大于阀值，那么修改其值
        :param val:
        :return:
        """
        if self.typeDict:
            if "a" in self.typeDict and self.typeDict["a"] > 3:
                self.typeDict["a"] = 3
            if "b" in self.typeDict and self.typeDict["b"] > 3:
                self.typeDict["b"] = 3
            if "va" in self.typeDict and self.typeDict["va"] > 3:
                self.typeDict["va"] = 3
            if "vb" in self.typeDict and self.typeDict["vb"] > 3:
                self.typeDict["vb"] = 3
            if "c" in self.typeDict and self.typeDict["c"] > 1000:
                self.typeDict["c"] = 1000
            if "vc" in self.typeDict and self.typeDict["vc"] > 1000:
                self.typeDict["vc"] = 1000

    def keyCheck(self, key):
        """
        对uid和topic进行检查
        :param key:
        :return:
        """
        status = True
        if key:
            keyArr = key.split(",,,,")
            if len(keyArr) == 2:
                uid, topic = keyArr[0], keyArr[1]
                if uid.strip() == "":
                    status = False
            else:
                status = False
        else:
            status = False
        return status

    def outputTopicResult(self):
        """
        输出单个话题的统计结果
        :return:
        """
        if self.keyCheck(self.currentKey):
            self.valCheck()
            outVal = ""
            for type, num in self.typeDict.iteritems():
                outVal += "{0}:{1}||".format(type, str(num))
            sys.stdout.write("{0}\t{1}\n".format(self.currentKey, outVal.strip("||")))
        else:
            sys.stderr.write("reporter:counter:weibo,nullUid,1\n")

    def flush(self):
        """
        判断处理列表中缓存的数据
        :return:
        """
        if self.currentKey:
            self.outputTopicResult()
            self.currentKey = None


if __name__ == "__main__":
    reducer = Reducer()
    reducer.reduce(sys.stdin)
    reducer.flush()
