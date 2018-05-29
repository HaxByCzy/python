#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2017-11-21 
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



from opencc import OpenCC
def b():
    a = "100808"
    b = "1008080000c3f57dc930d6166be6e270772ff9"
    c = len(a)
    print len(a), b[c:]

if __name__ == "__main__":
    b()