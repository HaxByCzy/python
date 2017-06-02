# -*- coding: utf-8 -*-
"""
task	: ***
input	: ***
output	: ***
@author	: baoquanZhang
update time	: Wed Mar 15 15:57:57 2017
"""

import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
from pandas import Series, DataFrame
import pandas as pd
import numpy as np

def fun():
    s = Series(index = ["a","b", "c"], data = [1, 2, 3])
    s2 = Series(index = ["a","b", "d"], data = [3, 4, 3, 8, 9])
    s1 = set(s.index) + set(s2.index)
    print s1




if __name__ == "__main__":
	fun()

