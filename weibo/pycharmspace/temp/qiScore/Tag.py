#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: hdfs数据get到本地前，根据 HIT_BASE过滤
@time: 2017-09-04
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

class Hit_BaseFilter(Parser):

    def __init__(self):
        Parser.__init__(self)
        self.tagNullNum = 0
        self.tagNum = 0
        self.weiboNum = 0

    def processOneWeibo(self, fieldMap):
        sys.stderr.write("reporter:counter:weibo,weiboTotalNum,1\n")
        if "TAG" in fieldMap:
            sys.stderr.write("reporter:counter:weibo,tagNotNullNum,1\n")
            self.outputWeibo()
        else:
            sys.stderr.write("reporter:counter:weibo,tagNullNum,1\n")




if __name__ == "__main__":
    hbf = Hit_BaseFilter()
    for line in sys.stdin:
        hbf.processOneLine(line.strip())
    hbf.flush()

