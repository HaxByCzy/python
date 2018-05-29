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
import time
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)



def category():
    outFile = open(sys.argv[2], "w")
    # outFile = open("d://data//out.dat", "w")
    totalNum, errNum = 0, 0
    with open(sys.argv[1], "r") as inFile:
    # with open("d://data//result-tmp.dat", "r") as inFile:
        for line in inFile:
            topicArr = line.strip().split("\t")
            if len(topicArr) == 3:
                topic, md5 , classJson = topicArr[0], topicArr[1], topicArr[2]
                classDict = json.loads(classJson, encoding='utf8')
                if classDict:
                    if "object" in classDict:
                        objDict = classDict["object"]
                        if "create_at" in objDict:
                            createAt = objDict["create_at"]
                            date = None
                            try:
                                date = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(int(createAt)))
                            except BaseException:
                                sys.stderr.write("time2date err!!!")

                            outFile.write("{0}\t{1}\n".format(topic, date))
            else:
                errNum += 1

    outFile.close()


if __name__ == "__main__":
    # compare()
    category()
    # vipTopicClass()