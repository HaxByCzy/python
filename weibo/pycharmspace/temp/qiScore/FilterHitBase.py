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
from util.Parser import Parser
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

class Hit_BaseFilter(Parser):
    """
    上海集群数据转移到沙溪前 hdfs 数据，根据 HIT_BASE & 0x20 != 0 过滤
    """

    def processOneWeibo(self, fieldMap):
        if "HIT_BASE" in fieldMap:
            hit_base_str = fieldMap["HIT_BASE"]
            if hit_base_str:
                hit_base = int(hit_base_str)
                if (hit_base & 0x20) != 0:
                    self.outputWeibo()
        else:
            self.outputWeibo()

if __name__ == "__main__":
    hbf = Hit_BaseFilter()
    for line in sys.stdin:
        hbf.processOneLine(line.strip())

