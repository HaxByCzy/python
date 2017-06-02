# -*- coding: utf-8 -*-
"""
task	: 根据互信息，求每个年龄段的特征APP
input	: 用户app
output	: 每个年龄段的特征
@author	: baoquanZhang
update time	: 2017-03-20
"""

import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
from pandas import Series, DataFrame
import numpy as np
import pandas as pd
from numpy import math

totalUserNum = 71552

# 对选出的特征建立索引，以生成训练数据集
def featureIndex():
    outFileName = "featureMiTIndex03.dat"
    featureSer = Series({})
    ageTypeList = np.array(["A", "B", "C", "D", "E", "F", "G", "H"])
    
    print "mutual Infor..."
    for ageType in ageTypeList:
        oneTypeSer = Series.from_csv("D://data//mi" + ageType + ".dat", sep = "\t")
        print ageType
        for key, val in oneTypeSer.iteritems():
            featureSer[key] = 1
            
#    print "tfidf..."
#    for ageType in ageTypeList:
#        oneTypeSer = Series.from_csv("D://data//tfidf" + ageType + ".dat", sep = "\t")
#        print ageType
#        for key, val in oneTypeSer.iteritems():
#            featureSer[key] = 1
            
    print "ouputing..."
    allAppSer = Series.from_csv("D://data//allAppNum.dat", sep = "\t")
    index = 0
    with open("D://data//" + outFileName , "w") as outFile:
        for key, val in allAppSer.iteritems():
            if key in featureSer:
                index += 1
                outLine = key + "\t" +  str(index)
                outFile.write(outLine + "\n")
        

# 计算每个年龄类特征的tf-idf，取 >0 的70%
def tfIdf():
    idfSer = Series.from_csv(path = "D://data//allAppNum.dat", sep = "\t") 
    idfSer = 1.0 / idfSer 
    ageTypeNum = Series({"A" : 1221.0,"B" : 46149.0, "C" : 21213.0, "D" : 2406.0, "E" : 382.0, "F" : 57.0, "G" : 20.0, "H" : 104.0})
    for ageType in ageTypeNum.index :
        oneAgeSer = Series.from_csv(path = "D://data//app" + ageType + ".dat", sep = "\t") 
        oneAgeSer = oneAgeSer / ageTypeNum[ageType]
        oneAgeSer = oneAgeSer * idfSer
        oneAgeSer = oneAgeSer[oneAgeSer.notnull()]
        oneAgeSer.sort_values(ascending = False, inplace = True)
        perNum = oneAgeSer.count() * 0.4
        index = 0
        print "outputing...  " + str(oneAgeSer.count()) + "  " + str(perNum)
        with open("D://data//tfIdf" + ageType + ".dat", "w") as outFile:
            for key, val in oneAgeSer.iteritems():
                index += 1
                if index < perNum:
                    outLine = key + "\t" + str(val)
                    outFile.write(outLine + "\n")
    

# 计算每个年龄类特征的互信息，取 >0 的70%
def mutualInfor():
    allAgeSer = Series.from_csv(path = "D://data//allAppNum.dat", sep = "\t") 
    allAgeSer = allAgeSer / totalUserNum
    ageTypeNum = Series({"A" : 1221.0,"B" : 46149.0, "C" : 21213.0, "D" : 2406.0, "E" : 382.0, "F" : 57.0, "G" : 20.0, "H" : 104.0})
    for ageType in ageTypeNum.index :
        oneAgeSer = Series.from_csv(path = "D://data//app" + ageType + ".dat", sep = "\t") 
        oneAgeSer = oneAgeSer / ageTypeNum[ageType]
        p_y = ageTypeNum[ageType] / totalUserNum
        outSer = Series({})
        index = 0
        for key, val in oneAgeSer.iteritems():
            if len(key.strip()) > 0 and key in allAgeSer :
                mi = val * p_y * (math.log10(val / allAgeSer[key]) / math.log10(2))
                if mi > 0:
                    outSer[key] = mi
                index += 1
                if index % 100 == 0 :print index,ageType, key,mi
        print ageType + " sorting..."
        outSer.sort_values(ascending = False, inplace = True)
        perNum = outSer.count() * 0.3
        print "outputing...  " + str(outSer.count()) + "  " + str(perNum)
        index = 0
        with open("D://data//mi" + ageType + ".dat", "w") as outFile:
            for key, val in outSer.iteritems():
                index += 1
                if index < perNum :
                    outLine = key + "\t" + str(val)
                    outFile.write(outLine + "\n")
            
                    
# 输出每个年龄的app个数
def oneAgeAppFeature():
    colNames = np.array(["userid", "age", "app"])
    rawDf = pd.read_table("D://data//ageJoinApp.dat", sep = "\t", names = colNames)
    ageTypes = rawDf["age"].unique()
    for ageType in ageTypes :
         oneAgeAppSer = rawDf[rawDf["age"]== ageType]["app"]
         appSer = Series({})
         index = 0
         for apps in oneAgeAppSer :
             index += 1
             print ageType,index
             appArr = np.array(apps.split(","))
             for app in appArr:
                 if app in appSer :
                     appSer[app] = appSer[app] + 1
                 else :
                     appSer[app] = 1
         appSer.sort_values(ascending = False, inplace = True)
         with open("D://data//app" + ageType + ".dat", "w") as outFile:
             for key, val in appSer.iteritems():
                 outLine = key + "\t" + str(val)
                 outFile.write(outLine + "\n")

         

# 输出所有app的个数，索引号
def allAppFeature():
    colNames = np.array(["userid", "age", "app"])
    rawDf = pd.read_table("D://data//ageJoinApp.dat", sep = "\t", names = colNames)
    appSer = Series({})
    appCol = rawDf["app"]
    i = 0
    for apps in appCol :
        i += 1
        if i % 100 == 0 : print i
        appArr = np.array(apps.split(","))
        for app in appArr:
            if app in appSer :
                appSer[app] = appSer[app] + 1
            else :
                appSer[app] = 1

    print "sort"
    appSer.sort_values(ascending = False, inplace = True)
    indexFile = open("D://data//allAppIndex.dat", "w")
    allAppNumFile = open("D://data//allAppNum.dat", "w")
    print "outputing"
    index = 1 
    for key, val in appSer.iteritems() :
        indexFile.write(key + "\t" + str(index) +"\n")
        allAppNumFile.write(key + "\t" + str(val) + "\n")
        index += 1
    indexFile.close()
    allAppNumFile.close()
    print "ok"
        

if __name__ == "__main__":
	#allAppFeature()
    #oneAgeAppFeature()
    #mutualInfor()
    #tfIdf()
    featureIndex()

