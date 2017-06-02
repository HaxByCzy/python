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
import redis

def getCommentAndLike(inputFile, outputFile):
    # 数据源链接
    con = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8', cursorclass=DictCursor)
    cur = con.cursor()
    redis_client = redis.Redis(host=redis_host, port=port)

    with open(outputFile, "w") as outFile:
        outFile.write("序号,作品id,作品名称,作者,总点赞数,总评论数" + "\n")
        with open(inputFile, "r") as inFile:
            for line in inFile:
                lineArray = line.strip().split("\t")
                index = lineArray[0]
                title = lineArray[1]
                author = lineArray[2]

                sql = "select id from topic where title = '{0}'".format(title)
                cur.execute(sql)
                result = cur.fetchone()
                if result != None:
                    topicId = result["id"]
                    print str(topicId) + "    " + title
                else:
                    print title + " has no topic_id !"

                like = redis_client.get('kuaikan-cache:likes:count:topic:%s' % topicId)
                comment = redis_client.get('kuaikan-cache:topic:%s:comments_count' % topicId)

                outLine = ",".join([index, str(topicId), title, author, like, comment])
                outFile.write(outLine.encode("utf-8") +"\n")
                print outLine
    cur.close()



if __name__ == "__main__":
    inputFile = sys.argv[1].strip()
    outputFile = sys.argv[2].strip()
    getCommentAndLike(inputFile, outputFile)
