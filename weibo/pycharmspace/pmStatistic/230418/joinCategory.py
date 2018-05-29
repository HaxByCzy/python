#-*- coding:utf-8 _*-  
""" 
@function: 
@time: 2017-07-04 
author:baoquan3 
@version: 
@modify: 
"""
import sys
import time


defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)


host = "10.73.12.142"
port = 6381


def b():
    categoryDict = {}
    dayDict = {}
    weekDict = {}

    with open(sys.argv[1], "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            if len(lineArr) == 2:
                categoryDict[lineArr[0]] = lineArr[1]

    outFile = open( sys.argv[2] , "w")
    with open(sys.argv[3], "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            if len(lineArr) == 2:
                topic = lineArr[0]
                month = lineArr[1]
                cateory = categoryDict[topic] if topic in categoryDict else "---"
                outLine = "{0}\t{1}\t{2}".format(topic, month, cateory)
                outFile.write(outLine + "\n")
    outFile.close()




if __name__ == "__main__":
    b()





