# -*- coding: utf-8 -*-
"""
task	: 使用car数据，计算每个特征的信息增益，和信息增益率
input	: car数据
output	: 特征信增益值
@author	: baoquanZhang
update time	: 2017-03-19 3：07 PM
"""

import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
from pandas import Series
import pandas as pd
import numpy as np
from numpy import math

# 计算信息增益率，从输出结果，可看出，信息增益率修正了，某特征项过多的情况
def inforGainRate(ig):
    #加载数据
    colNames = np.array(["buying", "maint", "doors", "persons", "lug_boot", "safety", "class"])
    carDf = pd.read_csv("dataCar//car_data.txt", sep = ",", names = colNames)
    #计算每个特征的信息熵
    featureEntropy = Series({})
    for elem in colNames[ : -1]:
        featureCol = carDf[elem]
        totalNum = featureCol.count() * 1.0
        featureItems = featureCol.unique()
        tmpEntr = 0
        for item in featureItems:
            tmp = featureCol[featureCol.values == item].count() / totalNum
            tmpEntr = tmpEntr + getEntropy(tmp)
        featureEntropy[elem] = tmpEntr
    inforGainRateSer = ig / featureEntropy
    print "==feature entropy=="
    print featureEntropy
    print "==================="
    print inforGainRateSer
            
# 计算互信息总函数，从输出结果中，可以看出，某个维度的特征的特征项越多，则它的信息增益越大
def inforGain():
    #加载数据
    colNames = np.array(["buying", "maint", "doors", "persons", "lug_boot", "safety", "class"])
    carDf = pd.read_csv("dataCar//car_data.txt", sep = ",", names = colNames)
    classCol = carDf["class"]
    #计算每个类别的熵，总熵和为1.0
    classEntropy = getClassEntropy(classCol)
    #计算每个特征的的条件熵
    inforGineSer = Series({})
    for elem in colNames[ : -1]:
        ig = classEntropy - getFeatureConditionEntropy(carDf[[elem, "class"]], elem)
        inforGineSer[elem] = ig
    inforGineSer.sort_values(ascending = False, inplace = True)
    print inforGineSer
    return inforGineSer
   

#计算一个特征的条件熵
def getFeatureConditionEntropy(featureAndClassDf, columnName) :
    #总类别数据量
    totalNum = featureAndClassDf["class"].count() * 1.0
    #类别名字
    classNames = featureAndClassDf["class"].unique()
    #所有特征的名字
    featuresNames = featureAndClassDf[columnName].unique()
    conditionEntropy = 0
    for feature in featuresNames :
        #得某特征下所有类
        claCol = featureAndClassDf[featureAndClassDf[columnName].values == feature]["class"]
        claNum = claCol.count()
        claPy = claNum / totalNum
        #得到某个特征发生的概率
        featurePorbability = featureAndClassDf[featureAndClassDf[columnName].values == feature][columnName].count() / totalNum
        for cla in classNames :
            tmpNum = claCol[claCol.values == cla].count() * 1.0
            if tmpNum != 0 :
                tmpEntr = claPy * getEntropy(tmpNum / claNum)
                conditionEntropy = conditionEntropy + tmpEntr * featurePorbability
    return conditionEntropy
        


#计算每个类别的熵  
def getClassEntropy(classCol):
    #每个类别的名字
    classNames = classCol.unique()
    #总数据量
    totalNum = classCol.count() * 1.0
    #计算熵
    classEntropy = 0
    for elem in classNames :
        oneClaNum = classCol[classCol.values == elem].count()
        tmpEntr = oneClaNum / totalNum 
        classEntropy = classEntropy + tmpEntr
        #print elem + " :  " + str(getEntropy(tmpEntr))
    return classEntropy
        

#熵计算
def getEntropy(p) :
    tmp = math.log10(p) / math.log10(2)
    return -(p * tmp)
    
   
   


if __name__ == "__main__":
    print "-----infor gain-----"
    ig = inforGain()
    print "-----infor gain rate----"
    inforGainRate(ig)
     
