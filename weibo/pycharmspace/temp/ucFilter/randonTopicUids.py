#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 随机选择若干话题
@time: 2018-03-16 
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

def topicFilter():
    index , total = 0, 150
    nums = "0123456789"
    for line in sys.stdin:
        index += 1
        if index % 50 != 0:
            continue
        line = line.strip()
        lineArr = line.split("\t")
        if len(lineArr) == 2:
            topic, uids = lineArr[0], lineArr[1]
            if len(unicode(topic)) > 15 or "[" in topic:
                continue
            status = True
            for num in nums:
                if num in topic:
                    status = False
                    break
            if status:
                uidArr = uids.split(",")
                if len(uidArr) > 1 and len(uidArr) < 10:
                    sys.stdout.write(line + "\n")


def filterTopic():
    print "aaaaaaaaa"
    outFile = open("D://data//outTopic.dat", "w")
    for line in open("d://data//topic.dat", "r"):
        line = line.strip()
        lineArr = line.split("\t")
        topic = lineArr[0]
        if len(unicode(topic)) > 15 or "[" in topic:
            continue
        status = True
        outFile.write(line + "\n")
        print line
    outFile.close()







if __name__ == "__main__":
    # topicFilter()
    filterTopic()