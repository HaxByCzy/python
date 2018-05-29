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

        uid = fieldMap["UID"] if "UID" in fieldMap and fieldMap["UID"] != "" else None

        if uid == None:
            return None

        tagTopic = None
        if "TAG_TOPIC" in fieldMap and fieldMap["TAG_TOPIC"] != "":
            tagTopic = fieldMap["TAG_TOPIC"]
        tagTopicArr = tagTopic.split("#")
        for elem in tagTopicArr:
            if elem:
                elemArr = elem.split("\t")
                for topic in elemArr:
                    if topic:
                        sys.stdout.write(uid + "\t" + topic + "\n")


if __name__ == "__main__":
    map = Mapper()
    for line in sys.stdin:
        map.processOneLine(line.strip())
    map.flush()
