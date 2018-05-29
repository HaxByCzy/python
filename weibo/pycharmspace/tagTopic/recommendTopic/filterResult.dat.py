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

def resultSelect():
    originScore = 1.0
    fwScore = 2.4
    originNum = 500
    fwNum = 1000
    outFile = open(sys.argv[2], "w")
    for line in open(sys.argv[1], "r"):
        lineArr = line.strip().split("\t")
        if len(lineArr) == 5:
            originNumTmp = int(lineArr[0]) if lineArr[0] != "null" else 0
            originScoreTmp = float(lineArr[1]) if lineArr[0] != "null" else 0
            fwNumTmp = int(lineArr[2]) if lineArr[0] != "null" else 0
            fwScoreTmp = float(lineArr[3]) if lineArr[0] != "null" else 0
            if originScoreTmp > 0 and fwScoreTmp > 0:
                if originScoreTmp < originScore and fwScoreTmp < fwScore:
                    if originNumTmp > originNum and fwNumTmp > fwNum:
                        outFile.write(line)
    outFile.close()


if __name__ == "__main__":
    resultSelect()