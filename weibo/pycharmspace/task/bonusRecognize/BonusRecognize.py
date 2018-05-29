#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 红包微博识别
@time: 2017-07-13 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
from WeiboParser import WeiboParser

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

class Weibo(object):
    """
    微博特征实例bean
    """

    def __init__(self):
        self.id = ""                 #微博id
        self.url = ""                #用户粉丝数
        self.content = ""            #微博内容
        self.filter = ""             #原创filter字段

class Bonus(WeiboParser):
    """
    发红包微博识别
    """
    id = "ID"
    url = "URL"
    content = "CONTENT"
    filter = "FILTER"

    titileTag = "#"

    def processWeibo(self):
        """
        根据一条微博相关特征，识别是否是属于发红包微博
        :return:
        """
        if len(self.tmpDict) > 0:
            wb = Weibo()
            if Bonus.id in self.tmpDict:
                wb.id = self.tmpDict[Bonus.id]
            if Bonus.url in self.tmpDict:
                wb.url = self.tmpDict[Bonus.url]
            if Bonus.content in self.tmpDict:
                wb.content = self.tmpDict[Bonus.content]
            if Bonus.filter in self.tmpDict:
                wb.filter = self.tmpDict[Bonus.filter]

            status = self.isBonus(wb)
            if status:
                self.outputReadWeibo(wb)
            self.tmpDict.clear()
            self.keyList = []

    def isBonus(self, wb):
        """
        根据微博特征，判断是否是发红包用户
        :param wb: 微博特征
        :return: true or false
        """
        keyword = True if wb.content.find("红包") != -1 and wb.content.find("我") != -1 else False
        if keyword:
            hyperlink = self.hasHyperlink(wb.content)
            if hyperlink:
                title = self.hasTitle(wb.content)
                if title:
                    orgin = False if int(wb.filter) & 4 != 0 else True
                    if orgin:
                        length = True if len(wb.content) > 10 else False
                        if length:
                            return True
                        else:
                            False
                    else:
                        return False
                else:
                    return False
            else:
                return False
        else:
            return False

    def hasTitle(self, line):
        """
        根据微博内容判断是否包含话题
        :param line: 微博内容
        :return: true or false
        """
        prefixIndex = line.find(Bonus.titileTag)
        if prefixIndex != -1:
            suffixIndex = line.find(Bonus.titileTag, prefixIndex + 1)
            if suffixIndex != -1:
                return True
        else:
            return False

    def hasHyperlink(self, line):
        """
        判断微博内容是否包含超链接
        :param line: 微博内容
        :return: true or false
        """
        prefixIndex = line.find("<sina:link src=")
        if prefixIndex != -1:
            suffixIndex = line.find("/>", prefixIndex + 1)
            if suffixIndex != -1:
                return True
        else:
            return False

    def outputReadWeibo(self, wb):
        """
        输出已识别出发红包用户微博，以方便阅读输出格式输出
        :param wb: 微博特征
        :return:
        """
        print "{0}\t{1}\t{2}\t{3}".format(wb.id,wb.filter, wb.url, wb.content)



if __name__ == "__main__":
    inFile = "D://data//wbTest.dat"
    bonus = Bonus()
    with open(inFile,"r") as f:
        for line in f:
            bonus.processOneRecord(line.strip())
    bonus.flush()