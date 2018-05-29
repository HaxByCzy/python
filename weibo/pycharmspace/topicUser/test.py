#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2017-08-03 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
from TopicUserMapper import TopicUserMapper

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

a = ""
a.fin

if __name__ == "__main__":
    """
    inFile = "D://data//part.dat"
    mp = TopicUserMapper()
    with open(inFile, "r") as inputFile:
        for line in inputFile:
            mp.map(line.strip())
    mp.flush()
    """
    inFile = "D://data//topicUser.dat"