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
from util.Parser import Parser
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

class Hit_BaseFilter(Parser):
    """
    清洗数据入库格式
    """

    def processOneWeibo(self, fieldMap):
        if "ID" in fieldMap and "HIT_BASE" in fieldMap and "TIME" in fieldMap:
            id = fieldMap["ID"]
            hit = fieldMap["HIT_BASE"]
            time = fieldMap["TIME"]
            if len(id.strip()) > 0 and len(hit.strip()) > 0 and len(time) > 0:
                sys.stdout.write("@\n")
                sys.stdout.write("@ID:" + id + "\n")
                sys.stdout.write("@HIT_BASE:" + hit + "\n")
                sys.stdout.write("@TIME:" + time + "\n")
                sys.stdout.write("@ACTION:M" + "\n")

if __name__ == "__main__":
    hbf = Hit_BaseFilter()
    for line in open("D://data//test.dat"):
        hbf.processOneLine(line.strip())
    hbf.flush()

