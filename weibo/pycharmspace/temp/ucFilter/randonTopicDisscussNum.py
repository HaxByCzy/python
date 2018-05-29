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
import urllib2
import hashlib
import json

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

def topicFilter():
    nums = "0123456789"
    engChar = "abcdefghigklmnopqrstuvwxyz"
    index , total = 0, 0
    baseUrl = url = "http://i2.api.weibo.com/2/darwin/topic/count.json?source=2936099636&ids=1022:100808"
    for line in sys.stdin:
    # for line in open("d://data//testUid.dat", "r"):
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
                md5 = hashlib.md5(topic.encode('utf-8')).hexdigest()
                req = urllib2.Request(baseUrl + md5)
                response = urllib2.urlopen(req)
                onlineJsonRes = json.loads(response.read(), encoding='utf8')[0]
                if "me" in onlineJsonRes:
                    onlineMe = int(onlineJsonRes["me"])
                    if onlineMe > 0 and onlineMe < 20:
                        sys.stderr.write(str(onlineMe) + "\t" + line + "\n")
                        total += 1
                        sys.stdout.write(line + "\n")







if __name__ == "__main__":
    topicFilter()