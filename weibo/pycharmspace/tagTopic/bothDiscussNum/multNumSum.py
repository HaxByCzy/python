#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-04-11 
@author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import time

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

inFileName = "d://data//{0}.dat"
outFileName = "d://data//{0}.dat"

def multTopicNumSum():
    allDays = 4
    for i in  range(2, allDays + 1):
        nameList = getFileName(i)
        outfile = outFileName.format(nameList[-1] + "-" + str(i))
        topicNumDict = getSumDict(nameList, i)
        outputRes(topicNumDict, outfile)


def getFileName(days):
    nameList = []
    timeStamp = int(time.time())
    hourSecond = 3600
    for i in range(1, days + 1):
        tmpTimeStamp = timeStamp - hourSecond * 24 * i
        timeFormat = time.strftime('%Y-%m-%d',time.localtime(tmpTimeStamp))
        nameList.append(timeFormat)
    return nameList

def getSumDict(fileNameList, days):
    numDict = {}
    if fileNameList:
        if days >= 2:
            readFile(outFileName.format(fileNameList[-2] + "-" + str(days - 1)), numDict)
            readFile(inFileName.format(fileNameList[-1]), numDict)
        else:
            for filename in fileNameList:
                readFile(inFileName.format(filename), numDict)
    return numDict

def readFile(filename, numDict):
    """
    将文件内容放到字典中
    :param filename:
    :param numDict:
    :return:
    """
    with open(filename, "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            if len(lineArr) == 7:
                topic = lineArr[0]
                originVld = int(lineArr[1]) if lineArr[1] != "--" else 0
                originInvld = int(lineArr[2]) if lineArr[2] != "--" else 0
                fwVld = int(lineArr[3]) if lineArr[3] != "--" else 0
                fwInvld = int(lineArr[4]) if lineArr[4] != "--" else 0
                rootVld = int(lineArr[5]) if lineArr[5] != "--" else 0
                rootInvld = int(lineArr[6]) if lineArr[6] != "--" else 0
                if topic not in numDict:
                    numDict[topic] = [0, 0, 0, 0, 0, 0]
                # 数据加和
                if originVld > 0:
                    numDict[topic][0] += originVld
                if originInvld > 0:
                    numDict[topic][1] += originInvld
                if fwVld > 0:
                    numDict[topic][2] += fwVld
                if fwInvld > 0:
                    numDict[topic][3] += fwInvld
                if rootVld > 0:
                    numDict[topic][4] += rootVld
                if rootInvld > 0:
                    numDict[topic][5] += rootInvld

def outputRes(outDict, filename):
    with open(filename, "w") as outFile:
        for topic, valList in outDict.iteritems():
            outLine = topic + "\t"
            for num in valList:
                if num > 0:
                    outLine += str(num) + "\t"
                else:
                    outLine += "--\t"
            outFile.write(outLine.strip() + "\n")


if __name__ == "__main__":
    multTopicNumSum()