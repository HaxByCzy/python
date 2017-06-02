# -*- coding: utf-8 -*-
"""
task	: 根据car数据，计算每个特征的基尼系统
input	: car.dat
output	: 特征用于反映纯度的基尼系统
@author	: baoquanZhang
update time	: 2017-03-27
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

# 衡量某个特征下类的不纯度，gini系数计算
def gini():
    giniSer = Series({})
    #加载数据
    colNames = np.array(["buying", "maint", "doors", "persons", "lug_boot", "safety", "class"])
    carDf = pd.read_csv("dataCar//car_data.txt", sep = ",", names = colNames)
    classNames = carDf["class"].unique()
    totalNum = carDf["class"].count() * 1.0
    #遍历每个特征
    for feature in colNames[:-1]:
        featureItems = carDf[feature].unique()
        featureGini = 0.0
        #遍历每个特征项
        for item in featureItems:
            itemCla = carDf[carDf[feature] == item]["class"] 
            itmeNum = itemCla.count() * 1.0
            #计算每个特征项的熵
            tmpGini = 0.0
            for cla in classNames:
                itemClaNum = itemCla[itemCla.values == cla].count()
                tmpGini = tmpGini + (itemClaNum / itmeNum) ** 2
            featureGini = featureGini + (itmeNum / totalNum) * (1 - tmpGini)
            print feature,item, (itmeNum / totalNum) * (1 - tmpGini)
            print "--------------"
        print "====================="
        giniSer[feature] = featureGini
    print "----------------gini ------------------"
    giniSer.sort_values()
    for key,val in giniSer.iteritems():
        print key + " : " + str(val)
                    
                    
                
                
            
            
            
            
        
        
        


if __name__ == "__main__":
	gini()

