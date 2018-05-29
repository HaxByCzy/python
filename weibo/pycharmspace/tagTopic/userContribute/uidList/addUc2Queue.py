#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-03-21 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import memcache
import hashlib

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)


def getContribute( line):
    """
    根据每行内容计算话题，对应用户的贡献度
    :param line:
    :return:(topic, uid, score)
    """
    lineArr = line.split("\t")
    if len(lineArr) == 2:
        uid, topic = "", ""
        keyArr = lineArr[0].split(",,,,")
        if len(keyArr) == 2:
            uid, topic = keyArr[0], keyArr[1]

        valArr = lineArr[1].split("||")
        typeDict = {}
        for elem in valArr:
            elemArr = elem.split(":")
            if len(elemArr) == 2:
                typeDict[elemArr[0]] = int(elemArr[1])
        score = 0
        if "a" in typeDict:
            score += 5 * typeDict["a"]
        if "b" in typeDict:
            score += 1 * typeDict["b"]
        if "c" in typeDict:
            if typeDict["c"] > 0:
                score += typeDict["c"] / 10
        if uid and topic:
            return (uid, topic, score)
        else:
            return None
    else:
        return None

inputHost, inputPort = "10.77.104.194", "11233",
inputKey = "contrib_async_mq"
def updateUidList():
    index = 0
    outFile = open(sys.argv[2], "w")
    mc = memcache.Client(["{0}:{1}".format(inputHost, inputPort)])
    with open(sys.argv[1], "r") as inFile:
        for line in inFile:
            index += 1
            uid, topic, score = getContribute(line.strip())
            topicMd5 = hashlib.md5(topic.encode('utf-8')).hexdigest()
            outLine = "{0}\t{1}\t{2}\t{3}".format("zadd", topicMd5, score, uid)
            mc.set(inputKey, outLine)
            outFile.write(str(index) + "\t" + line.strip() +  "\t"+  outLine + "\n")
    outFile.close()






if __name__ == "__main__":
    updateUidList()