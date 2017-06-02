# -*- coding: utf-8 -*-

import MySQLdb
import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
import MySQLdb
from MySQLdb.cursors import DictCursor
from setting import *

def getTopicFavourite(inFile):
    #从文件中读取作品名与id
    topicDict = {}
    with open(inFile) as file:
        for line in file:
            lineArray = line.strip().split(",")
            topicId = lineArray[1]
            title = lineArray[2]
            topicDict[topicId] = title

    # 数据源链接
    con = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8', cursorclass=DictCursor)
    cur = con.cursor()

    # 从comic表中获取每个作品的所有话
    topicComicDict = {}
    for topicId in topicDict:
        sql1 = "select id, title, created_at from comic where topic_id = {0} and status = 'published' order by id".format(topicId)
        print str(topicId), topicDict[topicId]
        cur.execute(sql1)
        result1 = cur.fetchall()
        length = len(result1)
        comicList = []
        for i in range(0, length):
            rowDict = result1[i]
            comicId = rowDict["id"]
            title = rowDict["title"]
            startTime = rowDict["created_at"]
            if i < length - 1:
                endTime = result1[i + 1]["created_at"]
            else:
                endTime = "2017-01-05 00:00:00"
            print "\t" + "   ".join([str(comicId), title, str(startTime),str(endTime)])
            comicList.append([str(comicId), title, str(startTime),str(endTime)])
        topicComicDict[topicId] = comicList

    # 抽取每话关注增涨量
    for topicId in topicComicDict:
        comicList = topicComicDict[topicId]
        topicTitle = topicDict[topicId].encode("utf-8")
        with open("./" + topicId + ".csv", "w") as file:
            file.write("序号,每话id,漫画名称,关注量,初始时间,结束时间" + "\n")
            index = 0
            for comic in comicList:
                index += 1
                comicId = comic[0]
                comicTitle = comic[1]
                startTime = comic[2]
                endTime = comic[3]
                sql2 = "SELECT count(target_id) as count from favourite where target_id = {0} and target_type = 1 and created_at BETWEEN '{1}' and  '{2}'".format(topicId, startTime, endTime)
                cur.execute(sql2)
                result2 = cur.fetchone()
                if result2 != None:
                    favCount = result2["count"]
                else:
                    favCount = 0
                    print sql2
                    print "can't get data!"
                outLine = ",".join([str(index), comicId, comicTitle, str(favCount), startTime, endTime])
                file.write(outLine.encode("utf-8") + "\n")
                print topicTitle + "\t" + outLine


    cur.close()



if __name__ == "__main__":
    inFile = sys.argv[1]
    getTopicFavourite(inFile)