#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2017-08-03 
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
from Parser import Parser


class MyParser(Parser):


    def processOneWeibo(self, fieldMap):
        print fieldMap["ID"]


if __name__ == "__main__":
    inFile = "D://data//part.dat"
    mp = MyParser()
    with open(inFile, "r") as inputFile:
        for line in inputFile:
            mp.processOneLine(line)
