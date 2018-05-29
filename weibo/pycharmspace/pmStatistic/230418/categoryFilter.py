#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-04-23 
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

def categoryFilter():
    category = "明星"
    outFile = open(sys.argv[1], "w")
    with open("./classNumTopic.dat", "r") as inFile:
    # with open("d://data//testIn.dat", "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            if len(lineArr) == 3:
                topic = lineArr[0]
                num = int(lineArr[1])
                topicCla = lineArr[2]
                if topicCla != "---" and topicCla == category:
                    if num >= 10:
                        outLine = "{0}\t{1}\t{2}".format(topicCla, num, topic)
                        outFile.write(outLine + "\n")

    outFile.close()

if __name__ == "__main__":
    categoryFilter()