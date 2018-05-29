#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 原样导出微博数据
@time: 2017-03-07
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


    def processOneWeiboModify(self, keyList, fieldMap):
        self.outputWeiboModify(keyList, fieldMap)


if __name__ == "__main__":
    map = Mapper()
    for line in sys.stdin:
        map.processOneLine(line.strip())
    map.flush()
