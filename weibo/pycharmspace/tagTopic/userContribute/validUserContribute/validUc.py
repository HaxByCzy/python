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

def getContribute(line):
    """
    根据每行内容计算话题，对应用户的贡献度
    :param line:
    :return:(topic, uid, score)
    """
    lineArr = line.split("\t")
    if len(lineArr) == 2:
        uid, topic = "", ""
        keyArr = lineArr[0].split(",,,,")
        if len(keyArr) == 2:
            uid, topic = keyArr[0], keyArr[1]

        valArr = lineArr[1].split("||")
        typeDict = {}
        for elem in valArr:
            elemArr = elem.split(":")
            if len(elemArr) == 2:
                typeDict[elemArr[0]] = int(elemArr[1])
        score = 0
        if "a" in typeDict:
            score += 5 * typeDict["a"]
        if "b" in typeDict:
            score += 1 * typeDict["b"]
        if "c" in typeDict:
            if typeDict["c"] > 0:
                score += typeDict["c"] / 10
        if uid and topic:
            return (uid, topic, score)
        else:
            return None
    else:
        return None

def getValidContribute(line):
    """
    根据每行内容计算话题，对应用户的贡献度
    :param line:
    :return:(topic, uid, score)
    """
    lineArr = line.split("\t")
    if len(lineArr) == 2:
        uid, topic = "", ""
        keyArr = lineArr[0].split(",,,,")
        if len(keyArr) == 2:
            uid, topic = keyArr[0], keyArr[1]

        valArr = lineArr[1].split("||")
        typeDict = {}
        for elem in valArr:
            elemArr = elem.split(":")
            if len(elemArr) == 2:
                typeDict[elemArr[0]] = int(elemArr[1])
        score = 0
        if "va" in typeDict:
            score += 5 * typeDict["va"]
        if "vb" in typeDict:
            score += 1 * typeDict["vb"]
        if "vc" in typeDict:
            if typeDict["vc"] > 0:
                score += typeDict["vc"] / 10
        if uid and topic:
            return (uid, topic, score)
        else:
            return None
    else:
        return None

def validUc():
    topicTime = {}
    with open(sys.argv[1], "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            if len(lineArr) == 2:
                topicTime[lineArr[0]] = lineArr[1]

    outFile = open(sys.argv[3], "w")
    with open(sys.argv[2], "r") as inFile:
        for line in inFile:
            result = getContribute(line.strip())
            if result:
                uid, topic, score = result
                if uid != "0":
                    if topic in topicTime:
                        vResult = getValidContribute(line.strip())
                        vscore = vResult[2]
                        outLine = "{0}\t{1}\t{2}\t{3}\n".format(line.strip(), topicTime[topic], score, vscore)
                        outFile.write(outLine)
    outFile.close()

if __name__ == "__main__":
    validUc()