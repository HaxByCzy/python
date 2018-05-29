#!/usr/bin/env python
# encoding=utf-8
#
import time
import sys
import json
import urllib2

reload(sys) 
sys.setdefaultencoding('utf8')

ac_base_url = "http://10.73.12.132/lib/libac1.php?ip=10.73.12.43&port=20002&sid=t_search&num=10&xsort=time&us=1&dup=0&category=0&cluster_repost=1&socialtime=1&isbctruncate=1&istag=2&nofilter=17&key="

outFile = open("D://data//topicMid.dat", "w")

def MidInfo(url, topic):
    index  = 0
    while True:
        try:
            index += 1
            url += "&start={0}".format(index)
            req = urllib2.Request(url)
            content = urllib2.urlopen(req)
            data = json.loads(content.read())
            if isinstance(data, dict) and data.has_key("sp") and isinstance(data["sp"], dict):
                if data["sp"].has_key("result"):
                    result = data["sp"]["result"]
                    if result:
                        for elem in result:
                            mid = elem["doc_id"]
                            sys.stdout.write(str(index) + "\t" + topic + "\t" + mid + "\n")
                            outFile.write(str(index) + "\t" + topic + "\t" + mid + "\n")
                    else:
                        break
        except:
            return "execpt!!!"

if __name__ == '__main__':
    if True:
        # f = open(sys.argv[1], "r")
        f = open("d://data//topic.dat", "r")
        while True:
            line = f.readline()
            if not line:
                break
            query = line.strip()
            url = ac_base_url + query.replace(" ", "%20")
            MidInfo(url, query)
            # print query + "\t" + str(m) + "\t" + str(m2)
        f.close()
        outFile.close()
