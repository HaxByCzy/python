#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-04-18 
@author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import math

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

def getInterList():
    keyList = []
    for i in range(1, 8):
        tmp = 10 ** i
        for j in range(1, 10):
            key = tmp * j
            keyList.append(key)
    keyList.append(100000000)
    return keyList


def topicNumCase2():
    keyDict = {}
    with open("d://data//a-part.dat", "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            if len(lineArr) == 2:
                key, num = lineArr[0], lineArr[1]
                keyDict[key] = num


    keyList = getInterList()
    outLine = "---\t" +  "\t".join(map(str, keyList))

    outFile = open("d://data//outMatrix2.dat", "w")
    outFile.write(outLine + "\n")
    for i in range(0, len(keyList)):
        outLine = str(keyList[i]) + "\t"
        for j in keyList:
            key = "{0}-{1}".format(keyList[i],j)
            if key in keyDict:
                outLine +=  keyDict[key] + "\t"
            else:
                outLine +=  "0\t"
            print outLine
        outFile.write(outLine.strip() + "\n")

    outFile.close()








def getIndex(num):
    num = str(num)
    colNum = int(num[0])
    num = int(num)
    rowNum = int(math.log(num / colNum, 10) - 1)
    index = rowNum * 9 + colNum - 1
    # print num, rowNum, colNum, index
    return index



if __name__ == "__main__":
    # topicNumCase()
    topicNumCase2()