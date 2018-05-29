#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-05-08 
@author:baoquan3 
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

def topic2tid():
    for line in sys.stdin:
        lineArr = line.strip().split("\t")
        if len(lineArr) == 2:
            topic = lineArr[0]
            tid = hashlib.md5(topic.encode('utf-8')).hexdigest()
            sys.stdout.write("100808" + tid + "\n")

if __name__ == "__main__":
    topic2tid()