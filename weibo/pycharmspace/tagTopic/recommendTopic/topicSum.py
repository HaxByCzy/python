#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-04-03 
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

def topicSum():
    outDict = {}
    for line in sys.stdin:
        lineArr = line.strip().split("\t")
        if len(lineArr) == 2:
            topic, num = lineArr[0], int(lineArr[1])
            if topic in outDict:
                outDict[topic] += num
            else:
                outDict[topic] = num
    outFile = open(sys.argv[1], "w")
    for topic, num in outDict.iteritems():
        outLine = "{0}\t{1}\n".format(topic, num)
        outFile.write(outLine)

    outFile.close()


if __name__ == "__main__":
    topicSum()