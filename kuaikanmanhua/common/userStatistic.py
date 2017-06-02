# -*- coding: utf-8 -*-

import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
import MySQLdb
from MySQLdb.cursors import DictCursor

db = 'kuaikan'
user = 'exp_imp_user'
passwd = 'G6_y7P9f5DpS8'
host = '10.9.5.43'

hotCount = 100
commentCount = 10

def geTtopicIdTopN():
    """
    从数据库中获取热度排名前100的用户
    :return:
    """
    #数据源链接
    con = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8', cursorclass=DictCursor)
    cur = con.cursor()

    #打开输出文件
    outFile = open("./topicId.txt", "w")

    #从数据库中取数据
    sql1 = "select topic_id, cnt from topic_click_count order by cnt desc;"
    cur.execute(sql1)
    results1 = cur.fetchall()

    index = 0
    for row in results1:

        #取前100
        index += 1
        if index > hotCount :
            break

        topicId = row["topic_id"]
        count = row["cnt"]
        sql2 = "select title from topic where id = {0}".format(topicId)
        cur.execute(sql2)
        title = cur.fetchone()["title"]
        outLine = ",".join([str(index), str(topicId), title, str(count)])
        print outLine
        outFile.write(outLine.encode("utf8") + "\n")


    #关闭
    outFile.close()
    cur.close()
    con.close()

def userCommentStatistic():
    # 数据源链接
    con = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8', cursorclass=DictCursor)
    cur = con.cursor()

    # 打开输出文件
    outFile = open("./userComment.csv", "w")
    outFile.write("id号,作品id,作品名称,热度,评论用户id,评论量,\n")

    # 从数据库中取数据
    with open("./topicId.txt", "r") as inFile:
        for line in inFile:
            line = line.strip()
            lineArray = line.split(",")
            if len(lineArray) >= 4:
                topicId = lineArray[1]

                sql1 = "select id from comic where topic_id = '%s'" % topicId
                cur.execute(sql1)
                results1 = cur.fetchall()
                userCommentCountDict = {}

                print lineArray[0]  + "    " + sql1
                #统计一个topicId的所有话
                for row1 in results1 :
                    comicId = row1["id"]
                    sql2 = "select user_id, count(user_id) as total from comment where comic_id = '{0}' group by user_id having total > '{1}' order by total desc".format(comicId, commentCount)
                    cur.execute(sql2)
                    results2 = cur.fetchall()

                    #统计一话的用户评论量
                    for row2 in results2:
                        userId = row2["user_id"]
                        comicComment = row2["total"]
                        if userId in userCommentCountDict:
                            userCommentCountDict[userId] = userCommentCountDict[userId] + comicComment
                        else:
                            userCommentCountDict[userId] = comicComment

                # #按评论量对用户排序
                # totalCommentCountList = sorted(userCommentCountDict.iteritems(), key = lambda d :d[1], reverse = True)
                # #文件输出
                # for tuple in totalCommentCountList:
                #     outLine = ",".join(lineArray) + ",{0},{1},".format(tuple[0], tuple[1])
                #     outFile.write(outLine.encode("utf8") + "\n")

                for key, value in userCommentCountDict.iteritems():
                    outLine = ",".join(lineArray) + ",{0},{1},".format(key, value)
                    outFile.write(outLine.encode("utf8") + "\n")


    outFile.close()
    cur.close
    con.close()



if __name__ == "__main__":
    #geTtopicIdTopN()
    userCommentStatistic()