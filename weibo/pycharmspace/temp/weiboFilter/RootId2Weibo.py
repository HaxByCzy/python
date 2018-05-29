#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: hdfs数据get到本地前，根据 HIT_BASE过滤
@time: 2017-08-08 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""

import sys
sys.path.append("./util")
sys.path.append("../util")
from Parser import Parser
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)


class Id2Weibo(Parser):
    """
    根据微博id抽取微博内容
    """
	
    def __init__(self):
        Parser.__init__(self)
        self.idList = []
        self.idSet = set()

    def readConfIdFile(self):
        """
        读取ID文件，放放set
        :return:
        """
        with open("id.dat", "r") as inFile:
            for line in inFile:
                id = line.strip()
                if id :
                    self.idList.append(id)
        self.idSet = set(self.idList)

    def processOneWeibo(self, fieldMap):
        if "ROOTMID" in fieldMap:
            id = fieldMap["ROOMID"]
            if id in self.idSet:
                self.outputWeibo()
                sys.stderr.write("reporter:counter:weibo,outputNum,1\n")


if __name__ == "__main__":
    i2w = Id2Weibo()
    i2w.readConfIdFile()
    for line in sys.stdin:
        i2w.processOneLine(line.strip())
    i2w.flush()

