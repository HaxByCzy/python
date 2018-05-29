#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: reduce端,微博格式转换并存储
@time: 2018-04-02
author:baoquan3 
@version: 
@modify:
--------------------------------------------------------------------
"""
import sys
import json

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
        self.currentVal = None          # 正处理的 val

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
        # for key, val in self.mapperData(open("d://data//test.dat", "r")):
            if key != self.currentKey:
                if self.currentKey:
                    sys.stderr.write("reporter:counter:weibo,weiboNum,1\n")
                    self.outputSingleTopicResult()
                    self.currentKey = key
                    self.currentVal = val
                else:
                    self.currentKey = key
                    self.currentVal = val
            else:
                    self.currentVal = val

    def outputSingleTopicResult(self):
        """
        输出单个话题的统计结果
        :return:
        """
        outLine = self.currentVal.replace("zhou####++##gong", "\t").replace("zhang==++==sang", "\n")
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
