# -*- coding: utf-8 -*-
"""
task	: ***
input	: ***
output	: ***
@author	: baoquanZhang
update time	: Fri May 05 09:23:23 2017
"""

import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
import numpy as np

def fun():
    x = np.array([[1,2,3],[4,5,6]])
    xt = x.T
    print x
    print "----------"
    print xt
    y = x.dot(xt)
    print y



if __name__ == "__main__":
	fun()

