#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 机器用户过滤得到符何要求的用户
@time: 2017-08-02 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)


def filterUser(line):
    lineArr = line.split("\t")
    if lineArr == "1":
        print line

if __name__ == "__main__":
    for line in sys.stdin:
        filterUser(line.strip())