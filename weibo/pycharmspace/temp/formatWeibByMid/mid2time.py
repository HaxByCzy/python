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
import time

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)



def id2time(inFilename, outFilename):
    outFile = open(outFilename, "w")
    with open(inFilename, "r") as inFile:
        for line in inFile:
            line = line.strip()
            tmp_time = time.localtime((int(line) >> 22) + 515483463)
            date = time.strftime("%Y-%m-%d",tmp_time)
            outFile.write(date + "\n")
    outFile.close()

if __name__ == "__main__":
    inFilename = sys.argv[1]
    outFilename = sys.argv[2]
    id2time(inFilename, outFilename)
