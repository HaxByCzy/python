# -*- coding: utf-8 -*-

import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
import MySQLdb
from MySQLdb.cursors import DictCursor
import redis

db = 'kuaikan'
user = 'exp_imp_user'
passwd = 'G6_y7P9f5DpS8'
host = '10.9.5.43'

redis_host = 'b-cache01'


def dataStatistic():
    #数据源链接
    con = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8', cursorclass=DictCursor)
    cur = con.cursor()
    redis_client = redis.Redis(host=redis_host)

    #打开输出文件
    outFile = open("./result.csv", "w")
    outFile.write("专题ID,作品名称,作品种类,作者名称,上架时间,更新状态,总热度,关注数,评论数,点赞数,\n")

    #根据输入文件从数据源取得数据
    with open("input_topic.txt", "r") as inFile:
        for line in inFile:
            line = line.strip()
            lineArray = line.split(";")
            if len(lineArray) >= 5:
                topicId = lineArray[0]
                sql = "select cnt from topic_click_count where topic_id = '{0}'".format(topicId)
                cur.execute(sql)
                results = cur.fetchall()

                #热度
                hotCount = "0"
                for row in results:
                    hotCount = row["cnt"]

                #关注数
                favCount = redis_client.get('kuaikan-cache:%s:fav:count' % topicId)

                sql2 = "select id from comic where topic_id = '{0}'".format(topicId)
                cur.execute(sql2)
                results2 = cur.fetchall()

                #评论数 与 点赞数
                commentCount = 0
                likeCount = 0
                for row in results2:
                    comicId = row["id"]
                    tmpCommentCount = redis_client.get('kuaikan-cache:comics:%s:comments_count' % comicId)
                    tmpLikeCount = redis_client.get('kuaikan-cache:likes:count:comic:%s' % comicId)
                    if tmpCommentCount is not None:
                        commentCount += long(tmpCommentCount)
                    if tmpLikeCount is not None:
                        likeCount += long(tmpLikeCount)

                #print topicId, hotCount, favCount, commentCount,likeCount
                outLine = ",".join(lineArray) + ",{0},{1},{2},{3},".format(hotCount, favCount, commentCount, likeCount)
                print outLine
                outFile.write(outLine.encode("utf8") + "\n")

    outFile.close()




if __name__ == '__main__':
    dataStatistic()
