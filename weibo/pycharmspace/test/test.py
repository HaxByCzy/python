#-*- coding:utf-8 _*-  
""" 
@function:
@time: 2017-07-04 
author:baoquan3 
@version: 
@modify: 
"""
import sys
import json


defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)


def b():
    with open("d://data//tmp.dat", "r") as inFile:
        for line in inFile:
            print line
            b = json.loads(line.strip(), encoding='utf8')
            for key, val in b.iteritems():
                print key



if __name__ == "__main__":
    b()





