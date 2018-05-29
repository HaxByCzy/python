#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-03-21 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import memcache
import hashlib

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)



inputHost, inputPort = "10.77.104.196", "11233",
inputKey = "topic_obj_baoquan3"
def updateUidList():
    mc = memcache.Client(["{0}:{1}".format(inputHost, inputPort)])
    index = 0
    # with open("d://data//topic-cover2.dat", "r") as inFile:
    with open(sys.argv[1], "r") as inFile:
        for line in inFile:
            index += 1
            line = line.strip()
            mc.set(inputKey, line)
            print index, line






if __name__ == "__main__":
    updateUidList()