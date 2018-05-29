#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 整合用户打分与用户特征解析两个模块，用于hadoop用户打分
@time: 2017-07-20 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
from WSParserJson import WeiboParserJson
from UserScore import UserScore

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

if __name__ == "__main__":
    inFile = "D://data//userjson.dat"
    paser = WeiboParserJson()
    us = UserScore()
    with open(inFile, "r") as input:
        for line in input:
            wb = paser.processOneWeibo(line.strip())
            score = us.calculateUserScore(wb)
            print wb.uid, "   ",score