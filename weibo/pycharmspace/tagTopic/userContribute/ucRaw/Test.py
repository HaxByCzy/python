#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-01-29 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import hashlib
import redis
from Mapper1 import UserContribute
import datetime
import time

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)





def check():
    a = "2010-10-01 00:00:28"
    timeArray = time.strptime(a, "%Y-%m-%d %H:%M:%S")
    timestamp = time.mktime(timeArray)
    print a , " --> ", int(timestamp)
    print len(str(time))
    uc = UserContribute()
    t1 = "4186776154039524"
    t2 = "4189977154059524"
    print len(t1)
    date1 , date2 = uc.getTimeById(t1), uc.getTimeById(t2)
    print date1
    print date2
    inteval = uc.getDateInterval(date1, date2)
    print inteval




if __name__ == "__main__":
   check()





