# -*- coding: utf-8 -*-
"""
task	: ***
input	: ***
output	: ***
@author	: baoquanZhang
update time	: Thu Mar 16 18:23:10 2017
"""

import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
from pandas import Series, DataFrame
import pandas as pd
import numpy as np
import math

inputFile = "D://data//uidAgeAppMerge.dat"
outputFile = "D://data//train20170526.dat"
indexFile = "D://data//featureMiTIndex.dat"
# 把用户的数据特征化
appNumSer = Series.from_csv(path = indexFile, sep = "\t")

def userApp2trainInFile():
    ageTypeSer = Series({"0" : 0, "1" : 1, "2" : 2, "3" : 3, "4" : 4, "5" : 5, "6" : 6, "7" : 7})
    with open(inputFile, "r") as inFile : 
        with open(outputFile, "w") as outFile :
            for line in inFile :
                lineArr = line.strip().split("\t")
                if len(lineArr) == 3 :
                    userid = lineArr[0]
                    ageType = ageTypeSer[lineArr[1]]
                    appTrait = app2trait(lineArr[2].split(","))
                    if len(appTrait) > 0 :
                        outLine = str(ageType) + " " + appTrait
                        #outLine = userid + "\t" + str(ageType) + " " + appTrait
                        outFile.write(outLine + "\n")
                    else :
                        print userid


# 将一个用户的APP做成训练输入特征                  
def app2trait(appArr):
    tmpDict = {}
    i = 0
    for app in appArr :
        if app in appNumSer :
            i += 1
            tmpDict[appNumSer[app]] = 1
    if i > 0 :
        appTraitSer = Series(tmpDict)
        appTraitSer.sort_index(ascending = True, inplace = True)
        outLine = ""
        for key, val in appTraitSer.iteritems():
            outLine = outLine + str(key) + ":" + str(val) + " "
        return outLine.strip()
    else : 
        return ""


if __name__ == "__main__":
	userApp2trainInFile()

