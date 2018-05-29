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
        if "ID" not in fieldMap:
            return

        if "QI" in fieldMap and len(fieldMap["QI"]) > 0:
            bit11 = int(fieldMap["QI"]) & 0x0800
            qi = bit11 | 0x0020
            fieldMap["QI"] = str(qi)
        else:
            return

        if "USER_TYPE" in fieldMap and len(fieldMap["USER_TYPE"]) > 0:
            user_type = int(fieldMap["USER_TYPE"]) & 0xFF72
            fieldMap["USER_TYPE"] = str(user_type)
        else:
            fieldMap["USER_TYPE"] = "0"
        sys.stdout.write("@\n")
        for key, val in fieldMap.iteritems():
            sys.stdout.write("@{0}:{1}\n".format(key, val))






if __name__ == "__main__":
    hbf = Hit_BaseFilter()
    for line in sys.stdin:
        hbf.processOneLine(line.strip())
    hbf.flush()

