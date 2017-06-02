# -*- coding: utf-8 -*-

import MySQLdb
import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

import MySQLdb
from MySQLdb.cursors import DictCursor

# 数据库链接
db = 'kuaikan'
user = 'exp_imp_user'
passwd = 'G6_y7P9f5DpS8'
host = '10.9.5.43'

con = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset='utf8', cursorclass=DictCursor)
cur = con.cursor()
sql ="""
select DISTINCT(b.topic_id) as topic_id, b.exclusive_flag, b.title as topic_title, a.updated_at from
(select topic_id, id as comic_id, title , updated_at from comic where updated_at >= '2016-08-01'  and updated_at < '2017-03-01') a
join
(SELECT id as topic_id, exclusive_flag, title, status from topic) b
on a.topic_id = b.topic_id
ORDER BY topic_id
"""
cur.execute(sql)
result = cur.fetchall()
lineSet = set([])
outFile = file("D://topicUpdate.csv", "w")
for row in result:
    topicId = row["topic_id"]
    exclusive = row["exclusive_flag"]
    title = row["topic_title"]
    time = str(row["updated_at"])[:10].replace("/","-")
    if exclusive == 1:
        outLine = str(topicId)  + "," + title+ ",独家," + time
    else:
        outLine = str(topicId)  + "," + title+ ",非独家," + time
    if outLine not in lineSet:
        outFile.write(outLine + "\n")
        lineSet.add(outLine)

    print outLine
outFile.close()




