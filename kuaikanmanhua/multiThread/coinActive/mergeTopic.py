# -*- coding: utf-8 -*-

import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
from setting import *

#合并发送邮件文件

def formatTimer(inFile):
    tmpDict = {}
    with open(inFile, "r") as inputFile:
        for line in inputFile:
            lineArr = line.strip().split(",")
            if len(lineArr) == 5:
                tmpDict[lineArr[0]] = ",".join(lineArr[1:])
    if len(tmpDict) != 3:
        tmpDict["hds"] = ",".join(["err", "err", "err", "err"])
        tmpDict["cqyt"] = ",".join(["err", "err", "err", "err"])
        tmpDict["bdwz"] = ",".join(["err", "err", "err", "err"])
    return tmpDict

def mergeOutFile(inputTime, buyChapter, topicTimer, outFile):
    topicDict = formatTimer(topicTimer)
    buyList = str(buyChapter).split(",")
    if len(buyList) != 3:
        buyList = ["err", "err", "err"]
    with open(outFile + "." + str(inputTime), "w") as outputFile:
        price = 0.68
        outLine = ",".join([str(inputTime), "花道士", str(price * float(buyList[0])), buyList[0], topicDict["hds"]])
        outputFile.write(outLine + "\n")
        outLine = ",".join([str(inputTime), "名花有草", str(price * float(buyList[1])), buyList[1], topicDict["cqyt"]])
        outputFile.write(outLine + "\n")
        outLine = ",".join([str(inputTime), "霸道王子的绝对命令", str(price * float(buyList[2])), buyList[2], topicDict["bdwz"]])
        outputFile.write(outLine + "\n")


if __name__ == "__main__":
    inputTime = "2017041410"
    buyChapter = "68,41,75"
    topicTimer = "D://data//topicTimer.dat.2017041410"
    outFile = "D://data//coinTopic.dat"

    # inputTime = sys.argv[1].strip()
    # buyChapter = sys.argv[2].strip()
    # topicTimer = sys.argv[3].strip()
    # outFile = sys.argv[4].strip()
    mergeOutFile(inputTime, buyChapter, topicTimer, outFile)

