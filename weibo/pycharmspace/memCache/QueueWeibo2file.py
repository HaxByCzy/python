#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 将相应队列数据导出备份
@time: 2018-03-09 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import memcache
import time
import json
import os

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

def bakQueue():
    """
    将队列数据保留到文件
    :return:
    """

    host , port = "queue.search.weibo.com", 11233
    mc194 = memcache.Client(['10.77.104.194:11233'])
    mc195 = memcache.Client(['10.77.104.195:11233'])
    mc196 = memcache.Client(['10.77.104.196:11233'])
    mc197 = memcache.Client(['10.77.104.197:11233'])
    mcList = [mc194, mc195, mc196, mc197]
    keyName = "huati_zbinfo+dataBak"

    fileTime =  time.strftime('%Y-%m-%d',time.localtime(time.time()))
    logFileName = "./dataRep/{0}.dat".format( fileTime)
    logFile = open(logFileName, "a")

    while True:
        valList = []
        for mc in mcList:
            val = mc.get(keyName)
            if val:
                valList.append(val)
        if len(valList) == 0:
            time.sleep(10)
        else:
            for val in valList:
                fileTime =  time.strftime('%Y-%m-%d',time.localtime(time.time()))
                logFileName = "./dataRep/{0}.dat".format( fileTime)
                if os.path.exists(logFileName) == False:
                    logFile.close()
                    logFile = open(logFileName, "a")

                if val:
                    logFile.write(val + "\n")
                    logFile.flush()


if __name__ == "__main__":
    bakQueue()
