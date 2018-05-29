#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-01-26 
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

import matplotlib.pyplot as plt
import numpy as np

xlabel = []
ylabel = []
total, midNum, status = 0, "", True
index = 0
tmp100 = 0
with open("D:data//contributeNum-sort.dat", "r") as inFile:
    for line in inFile:
        lineArr = line.strip().split("\t")
        if len(lineArr) == 2:
            index += 1
            if index > 100:
                tmp100 += int(lineArr[1])
            if index < 100:
                xlabel.append(int(lineArr[0]))
                ylabel.append(int(lineArr[1]))
            total += int(lineArr[1])
        if status and total >= 2669327298:
            midNum = line.strip()
            status = False
    print "midNum",midNum
    plt.plot(xlabel, ylabel)
    # plt.text(5, 4842750972, "5")
    # plt.text(10, 2443677904, "10")
    # plt.text(15, 235186750, "15")
    # plt.text(20, 352475489, "20")
    print tmp100, 100 * float(tmp100) / 5338654596
    plt.show()




