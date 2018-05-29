#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: reduce端处理模板
@time: 2017-08-25
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
    case 抽样 reduce 端处理
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
            if len(line.split(Reducer.SEP)) == 6:
                keyVal = line.split(Reducer.SEP, 1)
                yield keyVal

    def reduce(self, standardInput):
        """
        处理每行内容
        :return:
        """
        for key, val in self.mapperData(standardInput):
        #for key, val in self.mapperData(open("d://data//mapperOut.dat")):
            if key != self.currentKey:
                if self.currentKey:
                    self.function(self.currentKey, self.currentKeyList)
                    self.currentKey = key
                    self.currentKeyList = []
                    self.currentKeyList.append(val)
                else:
                    self.currentKey = key
                    self.currentKeyList.append(val)
            else:
                self.currentKeyList.append(val)

    def function(self, key, valList):
        """
        输出符合要求的数据
        :return:
        """
        pass

    def flush(self):
        """
        判断处理列表中缓存的数据
        :return:
        """
        if self.currentKey:
            self.caseCheck(self.currentKey, self.currentKeyList)


if __name__ == "__main__":
    reducer = Reducer()
    reducer.reduce(sys.stdin)
    reducer.flush()
