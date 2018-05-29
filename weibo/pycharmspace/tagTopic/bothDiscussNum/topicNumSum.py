#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 计算多日话题讨论数与有效讨论数的
@time: 2018-04-09 
@author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

def topicNumSum():
    outFile = open(sys.argv[1], "w")
    # outFile = open("d://data//ouput.dat", "w")
    allNumDict, validNumDict = {}, {}
    for line in sys.stdin:
    # for line in open("d://data//2018-04-08.dat", "r"):
        lineArr = line.strip().split("\t")
        if len(lineArr) == 3:
            topic = lineArr[0]
            allNum = int(lineArr[1]) if lineArr[1] != "---" else 0
            validNum = int(lineArr[2]) if lineArr[2] != "---" else 0
            if topic in allNumDict:
                allNumDict[topic] += allNumDict[topic]
            else:
                allNumDict[topic] = allNum
            if topic in validNumDict:
                validNumDict[topic] += validNumDict[topic]
            else:
                validNumDict[topic] = validNum
    print len(allNumDict), len(validNumDict), len(lineArr)

    for topic, allNum in allNumDict.iteritems():
        if allNum > 0:
            validNum = validNumDict[topic] if topic in validNumDict and validNumDict[topic] > 0 else "---"
            outLine = "{0}\t{1}\t{2}\n".format(topic, allNum, validNum)
            outFile.write(outLine)
    outFile.close()


if __name__ == "__main__":
    topicNumSum()