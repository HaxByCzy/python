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
sys.path.append("../util")
from Parser import Parser
import json
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

    def processOneWeiboModify(self, keyList, fieldMap):

        if "ID" in fieldMap and fieldMap["ID"] != "":
            mid  = fieldMap["ID"]
            outLine = "@zhang==++==sang"
            for key in keyList:
                if key in fieldMap:
                    outLine += "@{0}:{1}zhang==++==sang".format(key, fieldMap[key])
            outLine = outLine.replace("\t", "zhou####++##gong")
            sys.stdout.write("{0}\t{1}\n".format(mid, outLine))
            sys.stderr.write("reporter:counter:weibo,mapWeiboNum,1\n")

if __name__ == "__main__":
    map = Mapper()
    for line in sys.stdin:
        map.processOneLine(line.strip())
    map.flush()
