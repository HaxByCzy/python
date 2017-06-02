# -*- coding: utf-8 -*-

import MySQLdb
import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
import redis

redis_host = '10.9.138.156'
port = 6386
password = "F6p8T4sG3w9A7h"

like_redis_client = redis.Redis(host=redis_host, port=port, password = password)

comment_redis_client = redis.Redis(host="10.9.96.14", port=6388 )

def statistic(inFile, outFile):
    with open(outFile, "w") as outFile:
        with open(inFile, "r") as file :
            for line in file:
                lineArr = line.strip().split("\t")
                if len(lineArr) == 2:
                    topicId = lineArr[0]
                    title = lineArr[1]
                    likeNum = like_redis_client.get('kk:like:cnt:4:%s' % topicId)
                    outFile.write(topicId + "\t" + title + "\t" + str(likeNum) + "\n")
                    commentNum =  comment_redis_client.get('kuaikan-cache:topic:%s:comments_count' % topicId)
                    print topicId + "\t" + title + "\t" + str(commentNum)

if __name__ == "__main__" :
    inFile = sys.argv[1]
    outFile = sys.argv[2]
    statistic(inFile, outFile)


