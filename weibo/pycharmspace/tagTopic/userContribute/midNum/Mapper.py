#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 计算用户贡献度中位数
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

contributeDict = {}
# for line in open("D://data//testIn.dat", "r"):
for line in sys.stdin:
    lineArr = line.split("\t")
    if len(lineArr) == 2:
        valArr = lineArr[1].split("||")
        typeDict = {}
        for elem in valArr:
            elemArr = elem.split(":")
            if len(elemArr) == 2:
                typeDict[elemArr[0]] = int(elemArr[1])
        score = 0
        if "a" in typeDict:
            score += 10 * typeDict["a"]
        if "b" in typeDict:
            score += 5 * typeDict["b"]
        if "c" in typeDict:
            score += typeDict["c"] / 10

        if score in contributeDict:
            contributeDict[score] += 1
        else:
            contributeDict[score] = 1
    if len(contributeDict) >= 10000:
        for key, val in contributeDict.iteritems():
            sys.stdout.write("{0}\t{1}\n".format(key, val))
        contributeDict = {}

if contributeDict:
    for key, val in contributeDict.iteritems():
        sys.stdout.write("{0}\t{1}\n".format(key, val))
    contributeDict = {}



