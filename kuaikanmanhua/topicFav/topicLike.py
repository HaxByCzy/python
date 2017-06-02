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
print "get topic like num!"
redis_client = redis.Redis(host=redis_host, port=port, password = password)

face = redis_client.get('kk:like:cnt:4:544')
love =  redis_client.get('kk:like:cnt:4:782')


print "face : " + str(face)
print "love : " + str(love)




