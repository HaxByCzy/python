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
import math

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

def compute():
    days = 8.0

    # 计算每个话题均值
    averageDict = {}
    # with open("d://data//fw-sum.dat", "r") as inFile:
    with open(sys.argv[1], "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            if len(lineArr) == 2:
                avg = float(lineArr[1]) / days
                averageDict[lineArr[0]] = avg

    subDict = {}
    # with open("d://data//fw-test.dat", "r") as inFile:
    with open(sys.argv[2], "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            if len(lineArr) == 2:
                if lineArr[0] in averageDict:
                    avg = averageDict[lineArr[0]]
                    sub = math.pow(avg - float(lineArr[1]), 2)
                    if lineArr[0] in subDict:
                        subDict[lineArr[0]] += sub
                    else:
                        subDict[lineArr[0]] = sub

    # outFile = open("D://data//output.dat", "w")

    outDict = {}
    for key, val in subDict.iteritems():
        score = math.sqrt(val / days)
        if key in averageDict:
            outDict[key] = score / averageDict[key]

    outFile = open(sys.argv[3], "w")
    outList = sorted(outDict.iteritems(), key=lambda d:d[1], reverse = True)
    for elem in outList:
        outline = "{0}\t{1}\n".format(elem[1], elem[0])
        outFile.write(outline)



    outFile.close()







if __name__ == "__main__":
    compute()