#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 将话题词转成md5
@time: 2018-03-01 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import hashlib

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

start = len("1022:100808")
for line in sys.stdin:
    line = line.strip()
    if len(line) > 30:
        outline = line[start:]
        sys.stdout.write("{0}\t{1}\n".format(outline, 1))
    else:
        sys.stderr.write(line + "\n")
