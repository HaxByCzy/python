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
from Parser import Parser
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

class Mapper(Parser):
    """
    MapReduce的Map端处理
    """
    def processOneWeibo(self, fieldMap):
        pass

if __name__ == "__main__":
    mapper = Mapper()
    for line in sys.stdin:
        mapper.processOneLine(line.strip())
    mapper.flush()

