#-*- coding:utf-8 _*-
"""
--------------------------------------------------------------------
@function: 将相同uid的不同文件值排成多一行
@time: 2018-05-22
@author:baoquan3
@version:
@modify:
--------------------------------------------------------------------
"""
import sys

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

current_word = None
current_count = 0
word = None
ARGV_NUM = sys.argv[1]
arr=[0] * int(ARGV_NUM)

for i in range(int(ARGV_NUM)):
	arr[i]=0

for line in sys.stdin:
	line = line.strip('\n')

	word, count = line.split('\t', 1)

	try:
		count = int(count)
	except ValueError:
		continue

	if current_word == word:
		arr[count-1] = 1;
	else:
		if current_word:
			print "%s\t" %	(current_word),
			for i in range(int(ARGV_NUM)):
				if(i == int(ARGV_NUM)-1):
					print arr[i]
				else:
					print '%d\t' % (arr[i]),
			for i in range(int(ARGV_NUM)):
				arr[i] = 0
		arr[count-1] = 1
		current_word = word

if current_word == word:
	print "%s\t" %	(current_word),
	for i in range(int(ARGV_NUM)):
		if(i == int(ARGV_NUM)-1):
			print arr[i]
		else:
			print '%d\t' % (arr[i]),
