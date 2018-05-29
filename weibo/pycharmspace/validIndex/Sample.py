#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 抽样数据
@time: 2017-08-31 
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


uidSet = set()
index = 0
userList = []
lastUid = None
with open(sys.argv[1], "r") as inFile:
    for line in inFile:
        lineArr = line.strip().split("\t")
        uid = lineArr[0]
        if lastUid != uid:
            if userList:
                index += 1
                if index % 50 == 0:
                    uidSet.add(uid)
                    for elem in userList:
                        print elem
                userList = []
                lastUid = uid
                userList.append(line.strip())
            else:
                lastUid = uid
                userList.append(line.strip())
        else:
            userList.append(line.strip())
        if len(uidSet) >= 200:
            break

