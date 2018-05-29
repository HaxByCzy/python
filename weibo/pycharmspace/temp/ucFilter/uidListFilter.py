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
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

topicSet = set([])
with open("topic.dat") as inFile:
    for line in inFile:
        lineArr = line.strip().split("\t")
        topicSet.add(lineArr[0])

def ucFilter():
    for line in sys.stdin:
        line = line.strip()
        lineArr = line.split("\t")
        if len(lineArr) == 2:
            topic = lineArr[0]
            if topic not in topicSet:
                sys.stdout.write(line + "\n")


if __name__ == "__main__":
    ucFilter()

