#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 将话题词转成md5
@time: 2018-03-01 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import hashlib

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)


for line in sys.stdin:
    lineArr = line.strip().split("\t")
    if len(lineArr) == 2:
        topic = lineArr[0]
        md5 = hashlib.md5(topic.encode('utf-8')).hexdigest()
        sys.stdout.write("{0}\t{1}\n".format(md5, 1))