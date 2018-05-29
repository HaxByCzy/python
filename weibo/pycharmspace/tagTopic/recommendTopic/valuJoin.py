#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-04-02 
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

def joinValue():
    orginNumDict = {}
    with open(sys.argv[1], "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            if len(lineArr) == 2:
                orginNumDict[lineArr[0]] = lineArr[1]

    orginScoreDict = {}
    with open(sys.argv[2], "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            if len(lineArr) == 2:
                orginScoreDict[lineArr[1]] = lineArr[0]

    fwNumDict = {}
    with open(sys.argv[3], "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            if len(lineArr) == 2:
                fwNumDict[lineArr[0]] = lineArr[1]

    outFile = open(sys.argv[5], "w")
    with open(sys.argv[4], "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            if len(lineArr) == 2:
                topic = lineArr[1]
                score = lineArr[0]
                outline  = ""
                if topic in orginNumDict:
                    outline += str(orginNumDict[topic]) + "\t"
                else:
                    outline += "null\t"
                if topic in orginScoreDict:
                    outline += orginScoreDict[topic] + "\t"
                else:
                    outline += "null\t"
                if topic in fwNumDict:
                    outline += fwNumDict[topic] + "\t"
                else:
                    outline += "null\t"
                outline += score + "\t" + topic
                outFile.write(outline + "\n")





if __name__ == "__main__":
    joinValue()