#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 话题推荐计算原创有效话题讨论数
@time: 2017-04-02
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import json
sys.path.append("./util")
sys.path.append("../util")
from Parser import Parser
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

class Mapper(Parser):
    """
    TagTopic计算话题的讨论数
    """

    def __init__(self):
        Parser.__init__(self)

    def processOneWeibo(self, fieldMap):
        tagTmp = '{"队列测试话题数据":{"supply_list":[{"src":"6","timestamp":"1527061826","uid":"6086377672"}]}}'

        self.outputWeibo()
        sys.stdout.write("@TAG_TMP:{0}\n".format(tagTmp))


if __name__ == "__main__":
    mapper = Mapper()
    for line in sys.stdin:
        mapper.processOneLine(line.strip())
    mapper.flush()

