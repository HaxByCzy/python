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
    """
    质量分入索引库格式转换
    """

    def processOneWeibo(self, fieldMap):
        if "ID" in fieldMap and "QI" in fieldMap:
            id = fieldMap["ID"]
            qi = fieldMap["QI"]
            if len(id.strip()) > 0 and len(qi.strip()) > 0:
                # 取数值的 4 - 9 位数字， 0x03F0 : 0000 0011 1111 0000
                qi_4_9 = (int(qi) & 0x03F0) >> 4
                sys.stdout.write("@\n")
                sys.stdout.write("@ACTION:M\n")
                sys.stdout.write("@ID:" + id + "\n")
                sys.stdout.write("@_QI_4_9:" + str(qi_4_9) + "\n")

if __name__ == "__main__":
    hbf = Hit_BaseFilter()
    for line in sys.stdin:
        hbf.processOneLine(line.strip())
    hbf.flush()

