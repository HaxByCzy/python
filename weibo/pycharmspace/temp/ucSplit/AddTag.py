#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-03-08 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)


for line in sys.stdin:
    lineArr = line.strip().split("\t")
    if len(lineArr) == 2:
        topicUid, score = lineArr
        sys.stdout.write("{}\tyear=={}\n".format(topicUid, score))
