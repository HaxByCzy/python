#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-05-08 
@author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

topicList = []
def readConfIdFile():
    """
    读取ID文件，放放set
    :return:
    """
    with open("superTopic.dat", "r") as inFile:
        for line in inFile:
            topic = line.strip()
            topicList.append(topic)
    return set(topicList)

def removeSuperTopic():
    superTopicSet = readConfIdFile()
    for line in sys.stdin:
        lineArr = line.strip().split("\t")
        if len(lineArr) == 2:
            if lineArr[0] not in superTopicSet:
                sys.stdout.write(line.strip() + "\n")
                sys.stderr.write("reporter:counter:weibo,notSuperNum,1\n")
            else:
                sys.stderr.write("reporter:counter:weibo,superNum,1\n")

if __name__ == "__main__":
    removeSuperTopic()