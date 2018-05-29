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
    ipSuffix = "194"
    host , port = "10.77.104." + ipSuffix, 11233
    mc = memcache.Client(["{0}:{1}".format(host, port)])
    keyName = "contrib_async_mq+databak"

    fileTime =  time.strftime('%d',time.localtime(time.time()))
    logFileName = "./log/{0}-{1}.log".format(ipSuffix, fileTime)
    logFile = open(logFileName, "w")

    while True:
        val = mc.get(keyName)
        if val == None:
            time.sleep(10)
        else:
            if os.path.exists(logFileName) == False or os.path.getsize(logFileName) >= 157286400:
                logFile.close()
                fileTime =  time.strftime('%d',time.localtime(time.time()))
                logFileName = "./log/{0}-{1}.log".format(ipSuffix, fileTime)
                logFile = open(logFileName, "w")
            currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            logFile.write("{0}\t{1}\n".format(currentTime, val))

if __name__ == "__main__":
    bakQueue()
