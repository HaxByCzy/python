#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 将2015年7月份source为21320数据取出
@time: 2017-07-13 
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
        pass


class Weibo(object):
    """
    微博特征实例bean
    """

    def __init__(self):
        self.id = ""                     #微博id
        self.dupCont = ""                 #微博指纹
        self.content = ""                 #微博内容

import os
from ctypes import cdll

class TemplateRecongise(WeiboParser):
    """
    发红包微博识别
    """
    id = "ID"
    dupCont = "DUP_CONT_OLD"
    content = "CONTENT"

    def __init__(self):
        """
        初始空化模板字典
        :return:
        """
        WeiboParser.__init__(self)
        self.templateDict = {}

    def processWeibo(self):
        """
        根据一条微博相关特征，识别是否是属于发红包微博
        :return:
        """
        if len(self.tmpDict) > 0:
            wb = Weibo()
            if TemplateRecongise.id in self.tmpDict:
                wb.id = self.tmpDict[TemplateRecongise.id]
            if TemplateRecongise.dupCont in self.tmpDict:
                wb.dupCont = self.tmpDict[TemplateRecongise.dupCont]
            if TemplateRecongise.content in self.tmpDict:
                wb.content = self.tmpDict[TemplateRecongise.content]

            status = self.isMatch(wb)
            if status:
                #self.outputWeibo()
                self.outputReadWeibo(wb)
            self.tmpDict.clear()
            self.keyList = []

    def isMatch(self, wb):
        """
        根据微博特征，判断是否是发红包用户
        :param wb: 微博特征
        :return: true or false
        """
        return True

    def isTemplateWeibo(self,content, fingerprint):
        """
        根据指纹内容判断是否是模板微博
        :param content: 微博内容
        :param fingerprint: sim hash指纹内容
        :return: true or false
        """
        if len(content) < 40 or len(fingerprint) < 0:
            return False
        fpArr = fingerprint.split(" ")
        if len(fpArr) != 2:
            return False
        result = False
        for elem in fpArr:
            if result:
                break
            for i in range(2):
                key = (long(elem) >> (i * 16)) & 0xFFFF
                if key in self.templateDict:
                    diffBitNum = self.diffBitNum()

    def diffBitNum(self, fingerprint, fingerprintArr):
        """
        根据指纹和字典列表，计算差字节数
        :param fingerprint: 比较指纹
        :param fingerprintArr: 字典中列表
        :return: 不数的字节数
        """


    def loadTemplateDict(self, filePath):
        """
        根据字典文件路径，加载模板字典
        :param filePath: 模板字典路径
        :return: None
        """
        with open(filePath, "r") as inFile:
            for line in inFile:
                lineArr = line.strip().split("\t")
                fingerprint = lineArr[0]
                fingerprintArr = fingerprint.split(" ")
                if len(fingerprintArr) == 2:
                    for elem in fingerprintArr:
                        for i in range(2):
                            key = (long(elem) >> i * 16) & 0xFFFF
                            if key not in self.templateDict:
                                self.templateDict[key] = [fingerprint]
                            else:
                                self.templateDict[key].append(fingerprint)


    def outputReadWeibo(self, wb):
        """
        输出已识别出发红包用户微博，以方便阅读输出格式输出
        :param wb: 微博特征
        :return:
        """
        print "{0}\t{1}\t{2}".format(wb.id, wb.dupCont, wb.content)

    def flush(self):
        """
        将最后解析的微博放入weiboList中
        :return:
        """
        self.processWeibo()

if __name__ == "__main__":
    inFile = "D://data//part.dat"
    dictFile = "D://data//dict.dat"
    temp = TemplateRecongise()
    temp.loadTemplateDict(dictFile)
    for key, val in temp.templateDict.iteritems():
        print key
        for elem in val:
            print "    " + str(elem)
    # with open(inFile, "r") as input :
    #     for line in input:
    #         temp.processOneRecord(line.strip())


