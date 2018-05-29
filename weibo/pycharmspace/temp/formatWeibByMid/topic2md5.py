#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-03-07 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import hashlib

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

def topic2md5():
    outFile = open(sys.argv[2], "w")
    with open(sys.argv[1], "r") as inFile:
        for line in inFile:
            line = line.strip()
            md5 = hashlib.md5(line.encode('utf-8')).hexdigest()
            outFile.write("{0}\t{1}\n".format(line, md5))
    outFile.close()

def addTopicClass():

    topicDict = {}
    with open("D://data//topicTime.dat", "r") as inFile:
    # with open(sys.argv[1], "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            if len(lineArr) == 2:
                topic, cate = lineArr[0], lineArr[1]
                topicDict[topic] = cate

    # outFile = open(sys.argv[3], "w")
    # with open(sys.argv[2], "r") as inFile:
    with open("d://data//testIn.dat", "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            if len(lineArr) >= 6:
                topic = lineArr[6]
                cate = "null"
                if topic in topicDict:
                    cate = topicDict[topic]
                lineArr.insert(7, cate)
                outLine = "\t".join(lineArr)
                print line.strip()
                print outLine
                print
                # outFile.write(outLine + "\n")
            else:
                sys.stderr.write("line format err!")

    # outFile.close()

if __name__ == "__main__":
    # topic2md5()
    addTopicClass()
