#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 加和讨论数与有效讨论数
@time: 2018-04-25 
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

def sumNum():
    # for line in open("d://data//tmp.dat", "r"):
    for line in sys.stdin:
        lineArr = line.strip().split("\t")
        if len(lineArr) == 3:
            topic = lineArr[0]
            allNum = lineArr[1]
            validNum = lineArr[2]
            outLine = "{0}\t{1},{2}".format(topic, allNum, validNum)
            sys.stdout.write(outLine + "\n")



if __name__ == "__main__":
    sumNum()