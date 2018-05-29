#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-03-22 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import time

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

def outConfTime():
    a = "2018-04-13 18:00:00"
    timeArray = time.strptime(a, "%Y-%m-%d %H:%M:%S")
    timestamp = time.mktime(timeArray)

    begin = 1523613600
    outFile = open("d://data//confTime.dat", "w")
    for i in range(0, 70):
        stamp = begin - i * 600
        startStamp = stamp - 600
        endStamp = stamp -1
        day = time.strftime('%Y-%m-%d',time.localtime(startStamp))
        startTime = time.strftime('%H:%M:%S',time.localtime(startStamp))
        endTime = time.strftime('%H:%M:%S',time.localtime(endStamp))
        fileName = time.strftime('%Y-%m-%d-%H-%M',time.localtime(endStamp + 1))
        outLine = "{0}\t{1}\t{2}\t{3}".format(day, startTime, endTime, fileName)
        outFile.write(outLine + "\n")
        print outLine

if __name__ == "__main__":
    outConfTime()