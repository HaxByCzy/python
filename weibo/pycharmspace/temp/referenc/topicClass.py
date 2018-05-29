#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-02-12 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import urllib2
import hashlib
import json

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

def compare():
    """
    对比讨论数
    :return:
    """
    outFile = open(sys.argv[2], "w")
    totalNum, errNum = 0, 0
    with open(sys.argv[1], "r") as inFile:
        for line in inFile:
            topicArr = line.strip().split("\t")
            if len(topicArr) == 2:
                topic, num = topicArr[0], topicArr[1]
                md5 = hashlib.md5(topic.encode('utf-8')).hexdigest()
                outFile.write("{0}\t{1}\n".format(topic, md5))
            else:
                errNum += 1

    outFile.close()

def category():
    outFile = open(sys.argv[2], "w")
    # outFile = open("d://data//out.dat", "w")
    totalNum, errNum = 0, 0
    with open(sys.argv[1], "r") as inFile:
    # with open("d://data//result-tmp.dat", "r") as inFile:
        for line in inFile:
            topicArr = line.strip().split("\t")
            if len(topicArr) == 3:
                topic, num , classJson = topicArr[0], topicArr[1], topicArr[2]
                classDict = json.loads(classJson, encoding='utf8')
                if classDict:
                    if "object" in classDict:
                        objDict = classDict["object"]
                        if "category" in objDict:
                            category = objDict["category"]
                            if category:
                                cate = category.split("|")[0]
                                outFile.write("{0}\t{1}\n".format(topic, cate))
            else:
                errNum += 1

    outFile.close()

def getContribute( val):
    """
    根据每行内容计算话题，对应用户的贡献度
    :param line:
    :return:(topic, uid, score)
    """
    valArr = val.split("||")
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
        if typeDict["c"] > 0:
            score += typeDict["c"] / 10
    return score

def vipTopicClass():

    cateDict = {}
    with open(sys.argv[2], "r") as inFile:
    # with open("d://data//topicVipCate.dat", "r") as inFile:
        for line in inFile:
            lineArr = line.strip().split("\t")
            if len(lineArr) == 2:
                cateDict[lineArr[0]] = lineArr[1]

    outFile = open(sys.argv[3], "w")
    # outFile = open("d://data//out.dat", "w")
    totalNum, errNum = 0, 0
    with open(sys.argv[1], "r") as inFile:
    # with open("d://data//02.dat", "r") as inFile:
        for line in inFile:
            topicArr = line.strip().split("\t")
            if len(topicArr) == 2:
                key, val = topicArr[0], topicArr[1]
                keyArr = key.split(",,,,")
                a,b,c = 0, 0, 0
                if len(keyArr) == 2:
                    uid, topic = keyArr[0], keyArr[1]
                    topicCla = None
                    if topic in cateDict:
                        topicCla = cateDict[topic]
                    score = getContribute(val)
                    valArr = val.split("||")
                    for elem in valArr:
                        elemArr = elem.split(":")
                        cla, num = elemArr[0], int(elemArr[1])
                        if cla == "a":
                            a = num
                        elif cla == "b":
                            b = num
                        elif cla == "c":
                            if num > 0 :
                                c = num
                    if topicCla:
                        outLine = "{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\n".format(topicCla, topic,  uid, score, a, b, c)
                        outFile.write(outLine)


    outFile.close()

if __name__ == "__main__":
    # compare()
    # category()
    vipTopicClass()