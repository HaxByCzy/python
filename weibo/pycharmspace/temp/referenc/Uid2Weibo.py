#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 缩减微博数据，抽取数据，以供分析
@time: 2017-08-22 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
sys.path.append("./util")
from Parser import Parser
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)


class Mapper(Parser):
    """
    根据Uid抽取用户微博
    """
    def __init__(self):
        Parser.__init__(self)
        self.uidList = []
        self.uidSet = set()

    def readConfIdFile(self):
        """
        读取ID文件，放放set
        :return:
        """
        with open("uid.dat", "r") as inFile:
            for line in inFile:
                id = line.strip()
                if id :
                    self.uidList.append(id)
        self.uidSet = set(self.uidList)

    def processOneWeiboModify(self, keyList, fieldMap):
        if "LABLE_WORD" in keyList:
            keyList.remove("LABLE_WORD")
        if "UID" in fieldMap:
            if fieldMap["UID"] in self.uidSet:
                self.outputWeiboModify(keyList, fieldMap)


if __name__ == "__main__":
    map = Mapper()
    map.readConfIdFile()
    for line in sys.stdin:
        map.processOneLine(line.strip())
    map.flush()
