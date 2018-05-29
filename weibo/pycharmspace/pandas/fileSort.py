#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2018-05-29 
@author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
from pandas import Series

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

def fileSort():
    """
    给文件按数排序
    :return:
    """
    dataSer = Series.from_csv(path = "D://data//last2monthNoSuperTopics.dat", sep = "\t")
    sortSer = dataSer.sort_values(ascending = False)
    sortSer.to_csv(path = "D://data//last2monthNoSuperTopicsSort.dat", sep = "\t")
    print "end"

if __name__ == "__main__":
    fileSort()