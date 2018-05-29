#-*- coding:utf-8 _*-  
"""
--------------------------------------------------------------------
@function: 对微博的特征进行解析
@time: 2017-07-04 
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

class WeiboParser(object):
    """
    微博解析
    """
    recordSep = "@"         #单条微博的开始标识符
    fieldSep = ":"          #字段与值的分割符

    def __init__(self):
        self.keyList = []   #各个key的存储
        self.tmpDict = {}   #临时存放微博的原始字段值

    def processOneRecord(self, line):
        """
        单条微博特征解析
        :param line:
        :return:
        """
        if line == WeiboParser.recordSep:
            self.processWeibo()
        else:
            index = line.find(WeiboParser.fieldSep)
            if index != -1:
                fieldName = line[1: index].strip()
                val = line[index + 1 : len(line)]
                self.tmpDict[fieldName] = val
                self.keyList.append(fieldName)

    def processWeibo(self):
        """
        处理一条微博，由子类重写此方法
        :return:
        """
        pass

    def outputWeibo(self):
        """
        将微博原样输出
        :return:
        """
        print "@"
        for key in self.keyList:
             print "@{0}:{1}".format(key, self.tmpDict[key])

    def flush(self):
        """
        将最后解析的微博放入weiboList中
        :return:
        """
        self.processWeibo()

if __name__ == "__main__":
    inFile = "D://data//wbTest.dat"
    wp = WeiboParser()
    with open(inFile, "r") as input:
        for line in input:
            wp.processOneRecord(line.strip())
    wp.flush()

