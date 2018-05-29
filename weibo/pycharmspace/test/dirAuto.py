#-*- coding:utf-8 _*-  
""" 
@function: 
@time: 2017-07-04 
author:baoquan3 
@version: 
@modify: 
"""
import sys
from itertools import groupby
from operator import itemgetter
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
from math import  log



def b():
    list1 = [
        "2017-04-04",
        "2017-07-24",
        "2017-07-30",
        "2017-08-03",
        "2017-08-04",
        "2017-08-05",
        "2017-08-06",
        "2017-08-07",
        "2017-08-08",
        "2017-08-09",
        "2017-08-10"
        ]
    index = 1
    for i in range(0, len(list1)):
        tmp = list1[i].replace("-", "")
        out1 = "input_dir" + str(index) +"=/production/weibo/realtime/weibo/" + tmp + "/weibo/"
        #print out1
        index += 1
        out2 =  "input_dir" + str(index) +"=/production/weibo/realtime/weibo/" + tmp+"/2weibo/"
        index += 1
        #print out2
    index = 0
    for i in range(len(list1)):
        index += 1
        print "cmd=\"${cmd} -input '${input_dir"+ str(index) +"}'\""
        index += 1
        print "cmd=\"${cmd} -input '${input_dir"+ str(index) +"}'\""



def c():
    pass

if __name__ == "__main__":
    b()




