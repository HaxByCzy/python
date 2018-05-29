#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 将字典文件 写入到redis中
@time: 2017-07-24 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import redis

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

port = 6380

client0 = redis.Redis(host = "10.75.57.25", port = port)
client1 = redis.Redis(host = "10.75.57.28", port = port)

def file2redis(dictFile):
    with open(dictFile, "r") as input :
        for line in input:
            lineArr = line.strip().split("\t")
            if len(lineArr) == 2:
                key = lineArr[0]
                val = lineArr[1]
                index = getLinkIndex(key, 2)
                if index == 0:
                    client0.set(key, val)
                elif index == 1:
                    client1.set(key, val)
    print "sucess !"

def getLinkIndex(fingerprint, num):
    fp = long(fingerprint.split(" ")[0])
    index = fp % num
    return index

def writeSuccessTest():
    key0 = "3389200358 2855429895"
    key1 = "3256584833 2987090965"
    val0 = client0.get(key0)
    val1 = client1.get(key1)
    print "key0 : ", "3389200358 2855429895	打卡第3天,恢复锻炼 在#运动教室#热血开练,向着夏日好身材进击吧! ​​"
    print "key1 : ", "3256584833 2987090965	当妈真不容易,凌晨1点到现在才忙乎完,看看姑娘踉跄走着~我爱你宝贝儿~永远~ "
    print key0, " --- ", val0
    print key1, " --- ", val1



def testIndex(dictFile):
    index0 = 0
    index1 = 1
    index2 = 2
    other = 0
    with open(dictFile, "r") as input :
        for line in input:
            lineArr = line.strip().split("\t")
            if len(lineArr) == 2:
                key = lineArr[0]
                index = getLinkIndex(key, 3)
                if index == 0:
                    index0 += 1
                elif index == 1:
                    index1 += 1
                elif index == 2:
                    index2 +=1
                else:
                    other += 1

    print "index0 : ", index0, "index1 : ", index1, "index2 : ", index2, "other : ",other

if __name__ == "__main__":
    inFile = "D://data//dictFile.dat"
    #file2redis(inFile)
    writeSuccessTest()